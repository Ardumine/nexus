using HCore.Modules.Base;

namespace HCore.Packages.Nexus.SelfTest;

/// <summary>Snapshot the consumer exposes as a <c>recv_status</c> Cell facet.</summary>
public sealed record RecvStatus(long Received, long LastSequence, string State);

/// <summary>
/// Self-test consumer: subscribes to a scan_data facet by path — local or, in the
/// self-test, through a remote AFCP mount — and records progress on a
/// <c>recv_status</c> Cell facet. Nexus-internal replacement for the Sensor
/// RemoteSlam demo. The subscribe target is read from <see cref="TargetFile"/>.
/// </summary>
public sealed class TestConsumerImplement : BaseImplement, IRunnable
{
    /// <summary>VFS path the self-test writes the subscribe target into.</summary>
    public const string TargetFile = "/tmp/afcp_selftest_target";
    private const string DefaultTarget = "/proc/lidar/scan_data";

    private ISubscription? _sub;
    private IExposedData<RecvStatus>? _status;
    private long _received;
    private long _lastSequence = -1;

    public void Run()
    {
        var target = DefaultTarget;
        try
        {
            if (Vfs.Exists(TargetFile))
            {
                var configured = Vfs.ReadAllText(TargetFile).Trim();
                if (configured.Length > 0) target = configured;
            }
        }
        catch { /* fall back to default */ }

        _status = Data.ExposeData<RecvStatus>("recv_status", FacetKind.Cell, formatter: FormatStatus);
        Publish("Subscribing");
        _sub = Data.Subscribe<TestFrame>(target, OnFrame, OnDisconnected);
        Publish("Active");
    }

    private ValueTask OnFrame(DataEvent<TestFrame> e, CancellationToken ct)
    {
        _received++;
        _lastSequence = e.Sequence;
        Publish("Active");
        return ValueTask.CompletedTask;
    }

    private void OnDisconnected(DisconnectReason reason) => Publish(reason.ToString());

    private void Publish(string state) => _status?.Publish(new RecvStatus(_received, _lastSequence, state));

    private static string FormatStatus(RecvStatus s)
        => $"received={s.Received} lastSeq={s.LastSequence} state={s.State}";

    protected override void OnKilled() => _sub?.Dispose();
}

public sealed class TestConsumerDescriptor : IModuleDescriptor
{
    public string Name => "HCore.Packages.Nexus.TestConsumer";
    public string FriendlyName => "AFCP self-test consumer";
    public Type InterfaceType => typeof(IRunnable);
    public Type ImplementType => typeof(TestConsumerImplement);
}
