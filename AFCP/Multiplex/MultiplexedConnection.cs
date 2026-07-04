using System.Collections.Concurrent;
using AFCP.Streams;

namespace AFCP.Multiplex;

/// <summary>
/// A persistent, bidirectional, multiplexed channel over a single
/// <see cref="IFramedTransport"/>. Layer 2 of the AFCP stack. It lets many
/// logical request/response exchanges and one-way pushes share one transport
/// link, correlating each reply to its request by <see cref="Frame.RequestId"/>.
///
/// Threading model: one dedicated reader thread and one dedicated writer thread.
/// Callers send via <see cref="SendRequestAsync"/> / <see cref="SendNotify"/>
/// (which enqueue onto the writer); the reader dispatches incoming frames to
/// <see cref="OnRequest"/> / <see cref="OnNotify"/> handlers or completes the
/// pending <see cref="TaskCompletionSource{TResult}"/> for a response. This
/// mirrors V2's <c>ChannelManagerAfcpClientConector</c> but generalized and with
/// an explicit one-way <see cref="FrameKind.Notify"/> for push events.
/// </summary>
public sealed class MultiplexedConnection : IDisposable
{
    private readonly IFramedTransport _transport;
    private readonly bool _ownsTransport;
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<byte[]>> _pending = new();
    private readonly BlockingCollection<Frame> _outbox = new(new ConcurrentQueue<Frame>());

    private Thread? _readerThread;
    private Thread? _writerThread;
    private readonly CancellationTokenSource _cts = new();
    private uint _nextRequestId;
    private volatile bool _running;
    private readonly object _requestIdLock = new();

    /// <summary>
    /// Handler for incoming <see cref="FrameKind.Request"/> frames. Receives the
    /// message type and payload; returns the response payload (or null to send an
    /// empty response). Throwing surfaces to the caller as a faulted request.
    /// </summary>
    public Func<ushort, byte[], CancellationToken, byte[]?>? OnRequest { get; set; }

    /// <summary>
    /// Handler for incoming <see cref="FrameKind.Notify"/> frames (one-way push).
    /// </summary>
    public Action<ushort, byte[]>? OnNotify { get; set; }

    /// <summary>Fires when the link is closed (cleanly or on error).</summary>
    public event Action<ConnectionCloseReason>? Closed;

    public bool IsRunning => _running;

    public MultiplexedConnection(IFramedTransport transport, bool ownsTransport = true)
    {
        _transport = transport;
        _ownsTransport = ownsTransport;
    }

    public void Start()
    {
        if (_running) return;
        _running = true;
        _readerThread = new Thread(ReaderLoop) { Name = "AFCP Reader", IsBackground = true };
        _writerThread = new Thread(WriterLoop) { Name = "AFCP Writer", IsBackground = true };
        _readerThread.Start();
        _writerThread.Start();
    }

    /// <summary>Send a request and await the matching response.</summary>
    public Task<byte[]> SendRequestAsync(ushort messageType, byte[] payload, CancellationToken ct = default)
    {
        if (!_running) throw new InvalidOperationException("Connection is not running.");

        uint requestId;
        lock (_requestIdLock) { requestId = _nextRequestId++; }

        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[requestId] = tcs;

        ct.Register(() => tcs.TrySetCanceled(ct));

        var frame = new Frame(requestId, FrameKind.Request, messageType, payload);
        try
        {
            _outbox.Add(frame, _cts.Token);
        }
        catch (InvalidOperationException)
        {
            _pending.TryRemove(requestId, out _);
            throw new InvalidOperationException("Connection is closed.");
        }

        return tcs.Task;
    }

    /// <summary>Synchronous send-request helper for callers not on a sync-path that minds blocking.</summary>
    public byte[] SendRequest(ushort messageType, byte[] payload, TimeSpan? timeout = null)
    {
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource();
        return SendRequestAsync(messageType, payload, cts.Token).GetAwaiter().GetResult();
    }

    /// <summary>Send a one-way push. Never blocks on the remote side (no response is expected).</summary>
    public void SendNotify(ushort messageType, byte[] payload)
    {
        if (!_running) throw new InvalidOperationException("Connection is not running.");
        // Notify frames use RequestId = 0; the receiver never replies.
        _outbox.Add(new Frame(0, FrameKind.Notify, messageType, payload), _cts.Token);
    }

    private void ReaderLoop()
    {
        try
        {
            while (_running && !_cts.IsCancellationRequested)
            {
                Frame? maybe;
                try
                {
                    maybe = Frame.ReadFrom(_transport);
                }
                catch (EndOfStreamException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (Exception) when (!_running)
                {
                    break;
                }

                if (maybe is not Frame frame) break;

                switch (frame.Kind)
                {
                    case FrameKind.Response:
                        if (_pending.TryRemove(frame.RequestId, out var tcs))
                        {
                            tcs.TrySetResult(frame.Payload);
                        }
                        break;

                    case FrameKind.Request:
                        ThreadPool.QueueUserWorkItem(_ => HandleRequest(frame));
                        break;

                    case FrameKind.Notify:
                        var handler = OnNotify;
                        if (handler is not null)
                        {
                            ThreadPool.QueueUserWorkItem(_ =>
                            {
                                try { handler(frame.MessageType, frame.Payload); }
                                catch { /* notify handlers must not tear down the connection */ }
                            });
                        }
                        break;

                    default:
                        // Unknown frame kind — ignore for forward compatibility.
                        break;
                }
            }
        }
        finally
        {
            Shutdown(ConnectionCloseReason.RemoteClosed);
        }
    }

    private void HandleRequest(Frame frame)
    {
        byte[]? responsePayload = null;
        bool faulted = false;
        try
        {
            responsePayload = OnRequest?.Invoke(frame.MessageType, frame.Payload, _cts.Token);
        }
        catch
        {
            faulted = true;
        }

        if (!_running) return;
        var response = new Frame(frame.RequestId, FrameKind.Response, frame.MessageType, faulted ? Array.Empty<byte>() : (responsePayload ?? Array.Empty<byte>()));
        try
        {
            _outbox.Add(response, _cts.Token);
        }
        catch (InvalidOperationException)
        {
            // Connection closing; drop the response.
        }
    }

    private void WriterLoop()
    {
        try
        {
            foreach (var frame in _outbox.GetConsumingEnumerable(_cts.Token))
            {
                frame.WriteTo(_transport);
            }
        }
        catch
        {
            // Consumption ended or transport broken — reader loop will finalize.
        }
    }

    private void Shutdown(ConnectionCloseReason reason)
    {
        if (!_running) return;
        _running = false;
        try { _cts.Cancel(); } catch { /* ignore */ }
        try { _outbox.CompleteAdding(); } catch { /* ignore */ }

        foreach (var kv in _pending)
        {
            kv.Value.TrySetException(new InvalidOperationException($"AFCP connection closed ({reason})."));
        }
        _pending.Clear();

        if (_ownsTransport)
        {
            _transport.Dispose();
        }

        Closed?.Invoke(reason);
    }

    public void Close() => Shutdown(ConnectionCloseReason.LocalClosed);

    public void Dispose()
    {
        Shutdown(ConnectionCloseReason.LocalClosed);
        try { _cts.Dispose(); } catch { /* ignore */ }
        try { _outbox.Dispose(); } catch { /* ignore */ }
    }
}

public enum ConnectionCloseReason
{
    LocalClosed,
    RemoteClosed,
    Faulted,
}
