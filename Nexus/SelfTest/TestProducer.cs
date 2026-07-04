using HCore.Modules.Base;

namespace HCore.Packages.Nexus.SelfTest;

/// <summary>
/// Self-test producer: exposes a <c>scan_data</c> stream facet publishing synthetic
/// frames, and implements <see cref="ITestProbe"/> for the Layer-3 MKCall test.
/// A Nexus-internal replacement for the Sensor lidar demo, so the AFCP self-test
/// depends on neither the Sensor package nor HCore.Modules.Robotics.
/// </summary>
public sealed class TestProducerImplement : BaseImplement, ITestProbe, IRunnable
{
    private IExposedData<TestFrame>? _scan;
    private CancellationTokenSource? _cts;
    private int _frameRateHz = 10;

    public void Run()
    {
        _scan = Data.ExposeData<TestFrame>("scan_data", FacetKind.Stream, formatter: FormatFrame);
        _cts = new CancellationTokenSource();
        _ = Task.Run(() => PublishLoop(_cts.Token));
    }

    public void SetFrameRate(int hz) => _frameRateHz = hz;
    public int GetFrameRate() => _frameRateHz;
    public string GetName() => "afcp-test-probe";

    private async Task PublishLoop(CancellationToken ct)
    {
        var index = 0;
        while (!ct.IsCancellationRequested)
        {
            var samples = new int[16];
            for (var i = 0; i < samples.Length; i++) samples[i] = index + i;
            _scan!.Publish(new TestFrame(index++, samples));
            try { await Task.Delay(100, ct); }
            catch (OperationCanceledException) { break; }
        }
    }

    private static string FormatFrame(TestFrame f)
        => $"index:   {f.Index}\nsamples: [{f.Samples.Length}]";

    protected override void OnKilled() => _cts?.Cancel();
}

public sealed class TestProducerDescriptor : IModuleDescriptor
{
    public string Name => "HCore.Packages.Nexus.TestProducer";
    public string FriendlyName => "AFCP self-test producer";
    public Type InterfaceType => typeof(ITestProbe);
    public Type ImplementType => typeof(TestProducerImplement);
}
