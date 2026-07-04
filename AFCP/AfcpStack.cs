using AFCP.Streams;
using AFCP.Transport;

namespace AFCP;

/// <summary>
/// Builds the framed-transport stack that sits between a raw
/// <see cref="IConnection"/> and the <see cref="Multiplex.MultiplexedConnection"/>:
/// <c>IConnection → FramedTransport (length-prefix) → ChecksumFramedTransport (integrity)</c>.
/// Centralized so client and server build an identical stack.
/// </summary>
public static class AfcpStack
{
    public static IFramedTransport Build(IConnection connection, bool useChecksum = true, bool ownsConnection = true)
    {
        IFramedTransport framed = new FramedTransport(connection, ownsConnection);
        if (useChecksum)
        {
            framed = new ChecksumFramedTransport(framed, ownsInner: true);
        }
        return framed;
    }
}
