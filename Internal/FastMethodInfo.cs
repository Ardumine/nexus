using System.Linq.Expressions;
using System.Reflection;

namespace HCore.Packages.Nexus;

/// <summary>
/// Compiles a per-<see cref="MethodInfo"/> delegate that invokes the method by
/// boxing/unboxing its arguments, avoiding the per-call overhead of
/// <see cref="MethodInfo.Invoke"/>. Ported from V2's
/// <c>Kernel.AFCP.FastMethod.FastMethodInfo</c> for the MKCall server-side
/// dispatch path.
/// </summary>
internal sealed class FastMethodInfo
{
    public Func<object, object?[]?, object?> Invoke { get; }

    public FastMethodInfo(MethodInfo methodInfo)
    {
        var instanceParam = Expression.Parameter(typeof(object), "instance");
        var argsParam = Expression.Parameter(typeof(object[]), "arguments");

        var paramInfos = methodInfo.GetParameters();
        var argExprs = new Expression[paramInfos.Length];
        for (var i = 0; i < paramInfos.Length; i++)
        {
            argExprs[i] = Expression.Convert(
                Expression.ArrayIndex(argsParam, Expression.Constant(i)),
                paramInfos[i].ParameterType);
        }

        var call = Expression.Call(
            methodInfo.IsStatic ? null : Expression.Convert(instanceParam, methodInfo.DeclaringType!),
            methodInfo,
            argExprs);

        if (call.Type == typeof(void))
        {
            var voidDelegate = Expression.Lambda<Action<object, object?[]?>>(
                call, instanceParam, argsParam).Compile();
            Invoke = (instance, arguments) => { voidDelegate(instance, arguments); return null; };
        }
        else
        {
            Invoke = Expression.Lambda<Func<object, object?[]?, object?>>(
                Expression.Convert(call, typeof(object)),
                instanceParam, argsParam).Compile();
        }
    }
}
