using System.Net;
using System.Net.Sockets;

namespace AFCP.Transport;

/// <summary>
/// Accepts incoming <see cref="IConnection"/>s on a bound endpoint. The TCP
/// counterpart of the transport layer's listener role.
/// </summary>
public sealed class TcpTransportListener : IDisposable
{
    private readonly TcpListener _listener;
    private volatile bool _running;

    public TcpTransportListener(IPEndPoint endpoint)
    {
        _listener = new(endpoint);
    }

    public void Start()
    {
        _listener.Start();
        _running = true;
    }

    /// <summary>Block until a new connection arrives, then return it. Returns null if the listener was stopped.</summary>
    public IConnection? Accept(CancellationToken cancellationToken = default)
    {
        while (_running)
        {
            try
            {
                var client = _listener.AcceptTcpClient();
                return new TcpConnection(client);
            }
            catch (SocketException) when (!_running)
            {
                return null;
            }
            catch (SocketException) when (cancellationToken.IsCancellationRequested)
            {
                return null;
            }
        }
        return null;
    }

    public void Stop()
    {
        _running = false;
        try { _listener.Stop(); } catch { /* ignore */ }
    }

    public void Dispose() => Stop();
}

/// <summary>
/// Opens an outgoing <see cref="IConnection"/> to a remote endpoint.
/// </summary>
public static class TcpTransportClient
{
    public static IConnection Connect(IPEndPoint endpoint)
    {
        var client = new TcpClient();
        client.Connect(endpoint);
        return new TcpConnection(client);
    }

    public static async Task<IConnection> ConnectAsync(IPEndPoint endpoint, CancellationToken cancellationToken = default)
    {
        var client = new TcpClient();
        await client.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
        return new TcpConnection(client);
    }
}
