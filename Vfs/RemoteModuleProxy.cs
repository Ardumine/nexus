using AFCP;
using AFCP.Protocol;
using HCore.Modules.Base;
using System.Collections.Concurrent;
using System.Reflection;

namespace HCore.Packages.Nexus;

internal class RemoteModuleProxy<T> : DispatchProxy where T : class, IModule
{
    private AfcpClient _client = null!;
    private string _remotePath = null!;

    private static readonly ConcurrentDictionary<MethodInfo, (string Name, string[] ParamTypes)> s_signatures = new();

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        if (targetMethod is null)
            throw new ArgumentNullException(nameof(targetMethod));

        if (targetMethod.DeclaringType == typeof(object))
        {
            return targetMethod.Name switch
            {
                "ToString" => $"{typeof(T).Name} (remote @ {_remotePath})",
                "Equals" => ReferenceEquals(this, args?.FirstOrDefault()),
                "GetHashCode" => _remotePath.GetHashCode(StringComparison.Ordinal),
                _ => null,
            };
        }

        var (name, paramTypes) = s_signatures.GetOrAdd(targetMethod, BuildSignature);

        var request = new CallRequest
        {
            InstancePath = _remotePath,
            MethodName = name,
            ParamTypeNames = paramTypes,
            Args = args ?? Array.Empty<object?>(),
        };

        CallResponse response;
        try
        {
            response = _client.CallAsync(request, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch (Exception ex) when (ex is not RemoteCallException)
        {
            throw new RemoteCallException($"transport error calling '{name}' on '{_remotePath}': {ex.Message}", ex);
        }

        if (!response.Success)
        {
            throw new RemoteCallException(response.Error.Length > 0
                ? response.Error
                : $"remote call to '{name}' on '{_remotePath}' failed (no error detail).");
        }

        return response.ReturnValue;
    }

    private static (string Name, string[] ParamTypes) BuildSignature(MethodInfo method)
    {
        var parameters = method.GetParameters();
        var names = new string[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            names[i] = parameters[i].ParameterType.AssemblyQualifiedName
                       ?? parameters[i].ParameterType.FullName
                       ?? parameters[i].ParameterType.Name;
        }
        return (method.Name, names);
    }

    internal static T Create(AfcpClient client, string remotePath)
    {
        var proxy = (Create<T, RemoteModuleProxy<T>>() as RemoteModuleProxy<T>)!;
        proxy._client = client;
        proxy._remotePath = remotePath;
        return (proxy as T)!;
    }
}
