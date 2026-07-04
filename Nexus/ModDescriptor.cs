using HCore.Modules.Base;

namespace HCore.Packages.Nexus;

public sealed class ModDescriptor : IModuleDescriptor
{
    public string Name => "HCore.Packages.Nexus.Nexus";
    public string FriendlyName => "AFCP Nexus Connector";
    public Type InterfaceType => typeof(IAfcpKernel);
    public Type ImplementType => typeof(AfcpImplement);
}
