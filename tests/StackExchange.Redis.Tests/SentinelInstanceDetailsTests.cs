using System.Collections.Generic;
using Xunit;
using Xunit.Abstractions;

namespace StackExchange.Redis.Tests
{
    public class SentinelInstanceDetailsTests : TestBase
    {

        public SentinelInstanceDetailsTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        public void SentinelInstanceDetailsTest()
        {
            var kvps =
                new Dictionary<string, string>()
                {
                    { "name" , "mymaster"},
                    { "ip" , "127.0.0.1"},
                    { "port" , "6379"},
                    { "runid" , "953ae6a589449c13ddefaee3538d356d287f509b"},
                    { "flags" , "master"},
                    { "link-pending-commands" , "0"},
                    { "link-refcount" , "1"},
                    { "last-ping-sent" , "0"},
                    { "last-ok-ping-reply" , "735"},
                    { "last-ping-reply" , "735"},
                    { "down-after-milliseconds" , "5000"},
                    { "info-refresh" , "126"},
                    { "role-reported" , "master"},
                    { "role-reported-time" , "532439"},
                    { "config-epoch" , "1"},
                    { "num-slaves" , "1"},
                    { "num-other-sentinels" , "2"},
                    { "quorum" , "2"},
                    { "failover-timeout" , "60000"},
                    { "parallel-syncs" , "1"},
                };
            var instanceDetails = new ConnectionMultiplexer.SentinelInstanceDetails(kvps);
            Assert.Equal("mymaster", instanceDetails.Name);
            Assert.Equal("127.0.0.1", instanceDetails.Ip.ToString());
            Assert.Equal(6379, instanceDetails.Port);
            Assert.Equal("953ae6a589449c13ddefaee3538d356d287f509b", instanceDetails.RunId);
            Assert.Equal("master", instanceDetails.Flags);
            Assert.Equal(0, instanceDetails.LinkPendingCommands);
            Assert.Equal(1, instanceDetails.LinkReferenceCount);
            Assert.Equal(0, instanceDetails.LastPingSent);
            Assert.Equal(735, instanceDetails.LastOkPingReply);
            Assert.Equal(735, instanceDetails.LastPingReply);
            Assert.Equal(5000, instanceDetails.DownAfterMilliseconds);
            Assert.Equal(126, instanceDetails.InformationRefresh);
            Assert.Equal("master", instanceDetails.RoleReported);
            Assert.Equal(532439, instanceDetails.RoleReportedTime);
            Assert.Equal(1, instanceDetails.ConfigurationOfEpoch);
            Assert.Equal(1, instanceDetails.NumberOfSlaves);
            Assert.Equal(2, instanceDetails.NumberOfOtherSentinels);
            Assert.Equal(2, instanceDetails.Quorum);
            Assert.Equal(60000, instanceDetails.FailoverTimeout);
            Assert.Equal(1, instanceDetails.ParallelSynchronizations);
        }

    }
}
