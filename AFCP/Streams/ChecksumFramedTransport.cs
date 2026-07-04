using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AFCP.Streams;

/// <summary>
/// Integrity decorator over a base <see cref="IFramedTransport"/>. Appends a
/// 4-byte additive checksum after each framed message on write and verifies it
/// on read; a mismatch throws <see cref="InvalidDataException"/>. The checksum
/// covers only the payload (the base framer's length prefix is already
/// self-validating).
///
/// Layout on the wire per message:
/// <c>[base frame: uint32 len + payload][uint32 checksum of payload]</c>
/// </summary>
public sealed class ChecksumFramedTransport : IFramedTransport
{
    private readonly IFramedTransport _inner;
    private readonly bool _ownsInner;

    public ChecksumFramedTransport(IFramedTransport inner, bool ownsInner = true)
    {
        _inner = inner;
        _ownsInner = ownsInner;
    }

    public void WriteMessage(ReadOnlySpan<byte> message)
    {
        _inner.WriteMessage(message);
        Span<byte> checksumBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(checksumBytes, Checksum(message));
        // The checksum is itself framed by the base framer so the reader can
        // recover it as a discrete message.
        _inner.WriteMessage(checksumBytes);
    }

    public byte[]? ReadMessage()
    {
        var payload = _inner.ReadMessage();
        if (payload is null) return null;

        var checksumFrame = _inner.ReadMessage();
        if (checksumFrame is null) throw new EndOfStreamException("Connection closed before the checksum frame.");

        if (checksumFrame.Length != 4) throw new InvalidDataException($"Checksum frame must be 4 bytes, got {checksumFrame.Length}.");
        var expected = BinaryPrimitives.ReadUInt32LittleEndian(checksumFrame);
        var actual = Checksum(payload);
        if (expected != actual) throw new InvalidDataException($"Checksum mismatch: received {expected}, computed {actual}.");

        return payload;
    }

    public void Dispose()
    {
        if (_ownsInner) _inner.Dispose();
    }

    /// <summary>
    /// A fast additive 32-bit checksum over the bytes, accumulated in
    /// big-endian-ish word chunks (matches the reference implementation's
    /// behaviour; this is an integrity check, not a cryptographic hash).
    /// </summary>
    internal static uint Checksum(ReadOnlySpan<byte> data)
    {
        if (data.Length == 0) return 0;
        uint sum = 0;
        ref var p = ref MemoryMarshal.GetReference(data);
        var limit32 = data.Length - 32;
        var z = 0;
        while (z <= limit32)
        {
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z)));
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z + 4)));
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z + 8)));
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z + 12)));
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z + 16)));
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z + 20)));
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z + 24)));
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z + 28)));
            z += 32;
        }

        var limit4 = data.Length - 4;
        while (z <= limit4)
        {
            sum += BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, z)));
            z += 4;
        }

        var rem = data.Length - z;
        if ((rem & 3) >= 3) sum += (uint)Unsafe.Add(ref p, z + 2) << 8;
        if ((rem & 3) >= 2) sum += (uint)Unsafe.Add(ref p, z + 1) << 16;
        if ((rem & 1) == 1) sum += (uint)Unsafe.Add(ref p, z) << 24;
        return sum;
    }
}
