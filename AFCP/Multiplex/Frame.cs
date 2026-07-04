using System.Buffers.Binary;
using AFCP.Streams;

namespace AFCP.Multiplex;

/// <summary>
/// The kind of a multiplexed frame. Every frame on a <see cref="MultiplexedConnection"/>
/// carries a <see cref="Frame.RequestId"/> and one of these kinds so the receiver
/// knows whether to reply, complete a pending waiter, or hand a one-way push to a
/// handler.
/// </summary>
public enum FrameKind : byte
{
    /// <summary>A request expecting a <see cref="Response"/> with the same <see cref="Frame.RequestId"/>.</summary>
    Request = 0,

    /// <summary>The answer to a <see cref="Request"/>. Completes the pending waiter.</summary>
    Response = 1,

    /// <summary>
    /// A one-way, fire-and-forget push (no response). Used for server-initiated
    /// events such as live data frames for a subscription, or producer-killed
    /// notifications.
    /// </summary>
    Notify = 2,
}

/// <summary>
/// A single logical message on a <see cref="MultiplexedConnection"/>. Wire layout
/// (little-endian): <c>[uint32 RequestId][byte Kind][uint16 MessageType][payload bytes]</c>.
/// </summary>
public readonly record struct Frame(uint RequestId, FrameKind Kind, ushort MessageType, byte[] Payload)
{
    public const int HeaderSize = 4 + 1 + 2;

    public void WriteTo(IFramedTransport transport)
    {
        var buffer = new byte[HeaderSize + Payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(0, 4), RequestId);
        buffer[4] = (byte)Kind;
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(5, 2), MessageType);
        if (Payload.Length > 0)
        {
            Buffer.BlockCopy(Payload, 0, buffer, HeaderSize, Payload.Length);
        }
        transport.WriteMessage(buffer);
    }

    public static Frame? ReadFrom(IFramedTransport transport)
    {
        var raw = transport.ReadMessage();
        if (raw is null) return null;
        if (raw.Length < HeaderSize) throw new InvalidDataException($"Frame header truncated: got {raw.Length} bytes, need {HeaderSize}.");

        var requestId = BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(0, 4));
        var kind = (FrameKind)raw[4];
        var messageType = BinaryPrimitives.ReadUInt16LittleEndian(raw.AsSpan(5, 2));
        var payloadLength = raw.Length - HeaderSize;
        var payload = payloadLength == 0 ? Array.Empty<byte>() : raw[HeaderSize..];
        return new Frame(requestId, kind, messageType, payload);
    }
}
