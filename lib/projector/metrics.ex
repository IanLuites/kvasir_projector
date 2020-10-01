defmodule Kvasir.Projector.Metrics.Sender do
  @spec send(metric :: iodata()) :: :ok
  def send(metric)

  def send(metric) do
    require Logger
    Logger.debug(metric)

    :ok
  end
end

defmodule Kvasir.Projector.Metrics do
  @name :kvasir_projector_metrics
  @default_port 8125
  @supported_protocols ~W(statsd+udp statsd2+udp statsd statsd2 udp)

  @spec create :: :ok
  def create do
    if url = System.get_env("STATSD_URL") do
      unless Process.whereis(@name) do
        generate(url)
      end
    end

    :ok
  end

  @spec generate(String.t()) :: :ok
  defp generate(url) do
    {:ok, socket, header} = open(url)
    Process.register(socket, @name)

    fqdn =
      :net_adm.localhost()
      |> :net_adm.dns_hostname()
      |> elem(1)
      |> to_string()
      |> String.trim()
      |> String.downcase()

    tags =
      [
        "host:#{fqdn}",
        System.get_env("STATSD_TAGS")
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(",")

    Code.compile_quoted(
      quote do
        defmodule Kvasir.Projector.Metrics.Sender do
          @spec send(metric :: iodata()) :: :ok
          def send(metric)

          def send(metric) do
            Port.command(unquote(@name), [unquote(header), metric, unquote("," <> tags)])

            :ok
          end
        end
      end,
      "lib/projector/metrics/sender.ex"
    )

    :ok
  end

  @spec open(binary | URI.t()) :: {:ok, port, [byte, ...]}
  def open(url) do
    case URI.parse(url) do
      %URI{scheme: s, host: h, port: p} when s in @supported_protocols ->
        open(h, p || @default_port)

      _ ->
        raise "Invalid metrics url: #{inspect(url)}."
    end
  end

  @spec open(
          host :: binary | charlist | {byte, byte, byte, byte},
          port :: binary | integer
        ) :: {:ok, port, [byte, ...]}
  def open(host, port) do
    true = Code.ensure_loaded?(:gen_udp)

    h = if(is_binary(host), do: String.to_charlist(host), else: host)
    p = if(is_binary(port), do: String.to_integer(port), else: port)

    parent = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_udp.open(0, active: false)
      send(parent, {:socket, socket})
      :timer.sleep(:infinity)
    end)

    socket =
      receive do
        {:socket, s} -> s
      end

    {:ok, socket, build_header(h, p)}
  end

  ### UDP Building ###

  otp_release = :erlang.system_info(:otp_release)
  @addr_family if(otp_release >= '19', do: [1], else: [])

  defp build_header(host, port) do
    {ip1, ip2, ip3, ip4} =
      if is_tuple(host) do
        host
      else
        {:ok, ip} = :inet.getaddr(host, :inet)
        ip
      end

    anc_data_part =
      if function_exported?(:gen_udp, :send, 5) do
        [0, 0, 0, 0]
      else
        []
      end

    @addr_family ++
      [
        :erlang.band(:erlang.bsr(port, 8), 0xFF),
        :erlang.band(port, 0xFF),
        :erlang.band(ip1, 0xFF),
        :erlang.band(ip2, 0xFF),
        :erlang.band(ip3, 0xFF),
        :erlang.band(ip4, 0xFF)
      ] ++ anc_data_part
  end
end
