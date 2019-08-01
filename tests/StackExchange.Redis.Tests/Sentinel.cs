using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace StackExchange.Redis.Tests
{
    public class Sentinel : TestBase
    {
        private string ServiceName => TestConfig.Current.SentinelSeviceName;

        private ConnectionMultiplexer Conn { get; }
        private IServer Server { get; }
        protected StringWriter ConnectionLog { get; }

        public Sentinel(ITestOutputHelper output) : base(output)
        {
            ConnectionLog = new StringWriter();

            Skip.IfNoConfig(nameof(TestConfig.Config.SentinelServer), TestConfig.Current.SentinelServer);
            Skip.IfNoConfig(nameof(TestConfig.Config.SentinelSeviceName), TestConfig.Current.SentinelSeviceName);

            var options = new ConfigurationOptions()
            {
                CommandMap = CommandMap.Sentinel,
                EndPoints = { { TestConfig.Current.SentinelServer, TestConfig.Current.SentinelPort } },
                AllowAdmin = true,
                TieBreaker = "",
                ServiceName = TestConfig.Current.SentinelSeviceName,
                SyncTimeout = 5000
            };
            Conn = ConnectionMultiplexer.Connect(options, ConnectionLog);
            Thread.Sleep(3000);
            Assert.True(Conn.IsConnected);
            Server = Conn.GetServer(TestConfig.Current.SentinelServer, TestConfig.Current.SentinelPort);
        }

        [Fact]
        public void PingTest()
        {
            var test = Server.Ping();
            Log("ping took {0} ms", test.TotalMilliseconds);
        }

        [Fact]
        public void SentinelGetMasterAddressByNameTest()
        {
            var endpoint = Server.SentinelGetMasterAddressByName(ServiceName);
            Assert.NotNull(endpoint);
            var ipEndPoint = endpoint as IPEndPoint;
            Assert.NotNull(ipEndPoint);
            Log("{0}:{1}", ipEndPoint.Address, ipEndPoint.Port);
        }

        [Fact]
        public void SentinelGetMasterAddressByNameNegativeTest()
        {
            var endpoint = Server.SentinelGetMasterAddressByName("FakeServiceName");
            Assert.Null(endpoint);
        }

        [Fact]
        public async Task SentinelGetMasterAddressByNameAsyncNegativeTest()
        {
            var endpoint = await Server.SentinelGetMasterAddressByNameAsync("FakeServiceName").ForAwait();
            Assert.Null(endpoint);
        }

        [Fact]
        public void SentinelMasterTest()
        {
            var dict = Server.SentinelMaster(ServiceName).ToDictionary();
            Assert.Equal(ServiceName, dict["name"]);
            foreach (var kvp in dict)
            {
                Log("{0}:{1}", kvp.Key, kvp.Value);
            }
        }

        [Fact]
        public void SentinelSentinelsTest()
        {
            var sentinels = Server.SentinelSentinels("mymaster");
            Assert.True(sentinels[0].ToDictionary().ContainsKey("name"));
            foreach (var config in sentinels)
            {
                foreach (var kvp in config)
                {
                    Writer.WriteLine("{0}:{1}", kvp.Key, kvp.Value);
                }
            }
        }

        [Fact]
        public void SentinelMastersTest()
        {
            var masterConfigs = Server.SentinelMasters();
            Assert.True(masterConfigs[0].ToDictionary().ContainsKey("name"));
            foreach (var config in masterConfigs)
            {
                foreach (var kvp in config)
                {
                    Log("{0}:{1}", kvp.Key, kvp.Value);
                }
            }
        }

        [Fact]
        public void SentinelSlavesTest()
        {
            var slaveConfigs = Server.SentinelSlaves(ServiceName);
            if (slaveConfigs.Length > 0)
            {
                Assert.True(slaveConfigs[0].ToDictionary().ContainsKey("name"));
            }
            foreach (var config in slaveConfigs)
            {
                foreach (var kvp in config)
                {
                    Log("{0}:{1}", kvp.Key, kvp.Value);
                }
            }
        }

        [Fact]
        public async Task SentinelFailoverTest()
        {
            //  1/3: Get the master and slaves before requesting a failover
            var masterPort = (await Server.SentinelMasterAsync(ServiceName).ForAwait()).FirstOrDefault(kvp => kvp.Key == @"port").Value;
            var slavesPort = new HashSet<string>((await Server.SentinelSlavesAsync(ServiceName).ForAwait()).Select(kvps => kvps.FirstOrDefault(kvp => kvp.Key == @"port").Value));

            //  2/3: Request Sentinel for a forced failover
            await Server.SentinelFailoverAsync(ServiceName);

            //  3/3: Get the new master and slaves
            var masterPortAfterFailover = (await Server.SentinelMasterAsync(ServiceName).ForAwait()).FirstOrDefault(kvp => kvp.Key == @"port").Value;
            for (int index = 0; index < 60 && masterPort == masterPortAfterFailover; index++)
            {
                await Task.Delay(250);
                masterPortAfterFailover = (await Server.SentinelMasterAsync(ServiceName).ForAwait()).FirstOrDefault(kvp => kvp.Key == @"port").Value;
            }
            var slavesPortAfterFailover = new HashSet<string>((await Server.SentinelSlavesAsync(ServiceName).ForAwait()).Select(kvps => kvps.FirstOrDefault(kvp => kvp.Key == @"port").Value));

            //  Show pre and post configuration
            Log($"Master before failover: {masterPort}");
            foreach (var slavePort in slavesPort)
            {
                Log($"- Slave before failover: {slavePort}");
            }
            Log($"Master after failover: {masterPortAfterFailover}");
            foreach (var slavePortAfterFailover in slavesPortAfterFailover)
            {
                Log($"- Slave after failover: {slavePortAfterFailover}");
            }

            //  Assertions
            Assert.True(!string.IsNullOrWhiteSpace(masterPort), @"Master port before failover cannot be empty.");
            Assert.True(!string.IsNullOrWhiteSpace(masterPortAfterFailover), @"Master port after failover cannot be empty.");
            Assert.True(masterPort != masterPortAfterFailover, $@"Master must have changed (port: {masterPort}).");
            Assert.True(slavesPort.Count == slavesPortAfterFailover.Count, $@"Number of slaves must be equal after failover (before: {slavesPort.Count}, after: {slavesPortAfterFailover.Count}).");
            Assert.True(slavesPortAfterFailover.Contains(masterPort), $@"The old master must be a slave after failover (master: {masterPort}, slaves: [{string.Join(", ", slavesPortAfterFailover)}]).");
            Assert.True(slavesPort.Contains(masterPortAfterFailover), $@"The new master had to be a slave before failover (master: {masterPortAfterFailover}, slaves: [{string.Join(", ", slavesPort)}]).");
        }

        [Fact]
        public async Task SentinelSubscriberTest()
        {
            var key = $"{DateTime.UtcNow.Ticks}-Key";
            var value = $"{DateTime.UtcNow.Ticks}-Value";

            //  1/4 Get master and slave databases
            var configurationOptions = new ConfigurationOptions();
            var sentinelSubscriber = await this.Conn.CreateSentinelSubscriberAsync(configurationOptions).ForAwait();
            var writableDatabase = sentinelSubscriber.GetDatabase(this.ServiceName, SentinelDatabaseType.Master);
            var readonlyDatabase = sentinelSubscriber.GetDatabase(this.ServiceName, SentinelDatabaseType.Slave);

            //  Ensure that all operations (of other tests) are completed...
            await Task.Delay(1000).ForAwait();

            //  2/4 Write an entry in master (writable database)
            var result = await writableDatabase.StringSetAsync(key, value).ForAwait();

            //  3/4 Read the entry from slaves
            var replica = await readonlyDatabase.StringGetAsync(key).ForAwait();
            for (int index = 0; index < 60 && replica.IsNullOrEmpty; index++)
            {
                await Task.Delay(250).ForAwait();
                replica = await readonlyDatabase.StringGetAsync(key).ForAwait();
            }

            //  4/4 Try to write an entry in slaves (readonly database)
            Exception exception = null;
            try
            {
                await readonlyDatabase.StringSetAsync(key, value).ForAwait();
            }
            catch (Exception e) { exception = e; }

            //  Assertions
            Assert.True(result, "Cannot write in master database.");
            Assert.True(value == replica, "Cannot get the replica in slave database.");
            Assert.True(exception is RedisConnectionException, @"Can write in slave database.");
        }

        [Fact]
        public async Task SentinelSubscriberSwitchMasterTest()
        {
            //  1/3 Get master and slave databases before failover
            var configurationOptions = new ConfigurationOptions();
            var sentinelSubscriber = await this.Conn.CreateSentinelSubscriberAsync(configurationOptions).ForAwait();
            var writableDatabase = sentinelSubscriber.GetDatabase(this.ServiceName, SentinelDatabaseType.Master);
            var readonlyDatabase = sentinelSubscriber.GetDatabase(this.ServiceName, SentinelDatabaseType.Slave);

            //  Ensure that all operations (of other tests) are completed...
            await Task.Delay(1000).ForAwait();

            //  2/3 Request Sentinel for a forced failover
            SentinelSwitchMasterEventArgs eventArgs = null;
            sentinelSubscriber.SwitchMaster += (sender, e) => eventArgs = e;
            await Server.SentinelFailoverAsync(ServiceName);
            for (int index = 0; index < 60 && eventArgs == null; index++)
            {
                await Task.Delay(250);
            }

            //  3/3 Get master and slave databases after failover
            var writableDatabaseAfterFailover = sentinelSubscriber.GetDatabase(this.ServiceName, SentinelDatabaseType.Master);
            var readonlyDatabaseAfterFailover = sentinelSubscriber.GetDatabase(this.ServiceName, SentinelDatabaseType.Slave);

            //  Get the ports of endpoints
            var masterPort = writableDatabase.Multiplexer.GetEndPoints().Cast<IPEndPoint>().FirstOrDefault()?.Port ?? 0;
            var slavesPort = readonlyDatabase.Multiplexer.GetEndPoints().Cast<IPEndPoint>().FirstOrDefault()?.Port ?? 0;

            //  Show pre and post configuration
            //Log($"Master before failover: {masterPort}");
            //foreach (var slavePort in slavesPort)
            //{
            //    Log($"- Slave before failover: {slavePort}");
            //}
            //Log($"Master after failover: {masterPortAfterFailover}");
            //foreach (var slavePortAfterFailover in slavesPortAfterFailover)
            //{
            //    Log($"- Slave after failover: {slavePortAfterFailover}");
            //}

            ////  Assertions
            //Assert.True(!string.IsNullOrWhiteSpace(masterPort), @"Master port before failover cannot be empty.");
            //Assert.True(!string.IsNullOrWhiteSpace(masterPortAfterFailover), @"Master port after failover cannot be empty.");
            //Assert.True(masterPort != masterPortAfterFailover, $@"Master must have changed (port: {masterPort}).");
            //Assert.True(slavesPort.Count == slavesPortAfterFailover.Count, $@"Number of slaves must be equal after failover (before: {slavesPort.Count}, after: {slavesPortAfterFailover.Count}).");
            //Assert.True(slavesPortAfterFailover.Contains(masterPort), $@"The old master must be a slave after failover (master: {masterPort}, slaves: [{string.Join(", ", slavesPortAfterFailover)}]).");
            //Assert.True(slavesPort.Contains(masterPortAfterFailover), $@"The new master had to be a slave before failover (master: {masterPortAfterFailover}, slaves: [{string.Join(", ", slavesPort)}]).");
        }
    }
}
