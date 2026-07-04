using HCore.Modules.Base;

namespace HCore.Packages.Nexus.SelfTest;

/// <summary>
/// Minimal typed contract exercised by the AFCP Layer-3 MKCall self-test. Lives
/// inside Nexus so the self-test is fully self-contained — it is a single-process
/// loopback, so this type has one identity on both ends of the wire. Replaces the
/// former dependency on <c>HCore.Modules.Robotics.ILidar</c>.
/// </summary>
public interface ITestProbe : IModule
{
    void SetFrameRate(int hz);
    int GetFrameRate();
    string GetName();
}

/// <summary>
/// Demo facet payload for the self-test. The parameterless constructor is required
/// by the AFCP serializer (it constructs then sets properties).
/// </summary>
public sealed record TestFrame(int Index, int[] Samples)
{
    public TestFrame() : this(0, System.Array.Empty<int>()) { }
}
