using System.Buffers.Binary;
using AFCP.Transport;

namespace AFCP.Streams;

/// <summary>
/// Length-prefixed framing over a raw <see cref="IConnection"/>. Each message on
/// the wire is laid out as <c>[uint32 little-endian length][payload bytes]</c>.
/// This is the base framer — integrity (checksum) and transforms layer on top by
/// decorating an <see cref="IFramedTransport"/>.
/// </summary>
public sealed class FramedTransport : IFramedTransport
{
    private readonly IConnection _connection;
    private readonly bool _ownsConnection;

    public FramedTransport(IConnection connection, bool ownsConnection = true)
    {
        _connection = connection;
        _ownsConnection = ownsConnection;
    }

    public void WriteMessage(ReadOnlySpan<byte> message)
    {
        Span<byte> header = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(header, (uint)message.Length);
        _connection.Write(header);
        if (!message.IsEmpty)
        {
            _connection.Write(message);
        }
    }

    public byte[]? ReadMessage()
    {
        Span<byte> header = stackalloc byte[4];
        if (!ReadExact(header)) return null;

        var length = BinaryPrimitives.ReadUInt32LittleEndian(header);
        if (length == 0) return Array.Empty<byte>();
        // Guard against a corrupted/truncated frame claiming an absurd length.
        if (length > MaxMessageBytes) throw new InvalidDataException($"Frame length {length} exceeds the {MaxMessageBytes} byte limit.");

        var payload = new byte[length];
        if (!ReadExact(payload)) throw new EndOfStreamException("Frame truncated: connection closed mid-message.");
        return payload;
    }

    private bool ReadExact(Span<byte> buffer)
    {
        var remaining = buffer.Length;
        while (remaining > 0)
        {
            var read = _connection.Read(buffer[^remaining..]);
            if (read <= 0) return false;
            remaining -= read;
        }
        return true;
    }

    public void Dispose()
    {
        if (_ownsConnection)
        {
            _connection.Dispose();
        }
    }

    /// <summary>Hard cap on a single frame's declared length, to reject corrupt headers.</summary>
    public const int MaxMessageBytes = 64 * 1024 * 1024; // 64 MiB
}
