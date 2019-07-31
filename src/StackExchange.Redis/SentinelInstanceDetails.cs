using System.Collections.Generic;
using System.Net;

namespace StackExchange.Redis
{
    public partial class ConnectionMultiplexer
    {
        internal class SentinelInstanceDetails
        {

            public string Name { get; }
            public IPAddress Ip { get; }
            public ushort Port { get; }

            public string RunId { get; }
            public string Flags { get; }

            public int LinkPendingCommands { get; }
            public int LinkReferenceCount { get; }
            public int LastPingSent { get; }
            public int LastOkPingReply { get; }
            public int LastPingReply { get; }
            public int DownAfterMilliseconds { get; }
            public int InformationRefresh { get; }
            public string RoleReported { get; }
            public int RoleReportedTime { get; }
            public int ConfigurationOfEpoch { get; }
            public int NumberOfSlaves { get; }
            public int NumberOfOtherSentinels { get; }
            public int Quorum { get; }
            public int FailoverTimeout { get; }
            public int ParallelSynchronizations { get; }

            internal SentinelInstanceDetails(IEnumerable<KeyValuePair<string, string>> kvps)
            {
                foreach (var kvp in kvps)
                    switch (kvp.Key)
                    {
                        case @"name": this.Name = kvp.Value; break;
                        case @"ip": if (IPAddress.TryParse(kvp.Value, out var ip)) this.Ip = ip; break;
                        case @"port": if (ushort.TryParse(kvp.Value, out var port)) this.Port = port; break;
                        case @"runid": this.RunId = kvp.Value; break;
                        case @"flags": this.Flags = kvp.Value; break;
                        case @"link-pending-commands": if (int.TryParse(kvp.Value, out var linkPendingCommands)) this.LinkPendingCommands = linkPendingCommands; break;
                        case @"link-refcount": if (int.TryParse(kvp.Value, out var linkReferenceCount)) this.LinkReferenceCount = linkReferenceCount; break;
                        case @"last-ping-sent": if (int.TryParse(kvp.Value, out var lastPingSent)) this.LastPingSent = lastPingSent; break;
                        case @"last-ok-ping-reply": if (int.TryParse(kvp.Value, out var lastOkPingReply)) this.LastOkPingReply = lastOkPingReply; break;
                        case @"last-ping-reply": if (int.TryParse(kvp.Value, out var lastPingReply)) this.LastPingReply = lastPingReply; break;
                        case @"down-after-milliseconds": if (int.TryParse(kvp.Value, out var downAfterMilliseconds)) this.DownAfterMilliseconds = downAfterMilliseconds; break;
                        case @"info-refresh": if (int.TryParse(kvp.Value, out var informationRefresh)) this.InformationRefresh = informationRefresh; break;
                        case @"role-reported": this.RoleReported = kvp.Value; break;
                        case @"role-reported-time": if (int.TryParse(kvp.Value, out var roleReportedTime)) this.RoleReportedTime = roleReportedTime; break;
                        case @"config-epoch": if (int.TryParse(kvp.Value, out var configurationOfEpoch)) this.ConfigurationOfEpoch = configurationOfEpoch; break;
                        case @"num-slaves": if (int.TryParse(kvp.Value, out var numberOfSlaves)) this.NumberOfSlaves = numberOfSlaves; break;
                        case @"num-other-sentinels": if (int.TryParse(kvp.Value, out var numberOfOtherSentinels)) this.NumberOfOtherSentinels = numberOfOtherSentinels; break;
                        case @"quorum": if (int.TryParse(kvp.Value, out var quorum)) this.Quorum = quorum; break;
                        case @"failover-timeout": if (int.TryParse(kvp.Value, out var failoverTimeout)) this.FailoverTimeout = failoverTimeout; break;
                        case @"parallel-syncs": if (int.TryParse(kvp.Value, out var parallelSynchronizations)) this.ParallelSynchronizations = parallelSynchronizations; break;
                    }
            }

            public override string ToString() => $@"[{this.Ip}]:{this.Port} (""{this.Name}"" {this.Flags})";

        }
    }
}
