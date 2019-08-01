using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis
{

    /// <summary>
    /// The type of the database.
    /// </summary>
    public enum SentinelDatabaseType
    {
        /// <summary>
        /// Master database (writable).
        /// </summary>
        Master = 0,
        /// <summary>
        /// Slave database (readonly).
        /// </summary>
        Slave = 1,
    }

    /// <summary>
    /// Represents the abstract multiplexers API to Sentinel.
    /// </summary>
    public interface ISentinelSubscriber
    {
        /// <summary>
        /// Obtain an interactive connection to a database.
        /// </summary>
        /// <param name="serviceName">The sentinel service name.</param>
        /// <param name="databaseType">The type of the database.</param>
        /// <param name="db">The ID to get a database for.</param>
        /// <param name="asyncState">The async state to pass into the resulting <see cref="RedisDatabase"/>.</param>
        IDatabase GetDatabase(string serviceName, SentinelDatabaseType databaseType, int db = -1, object asyncState = null);
    }

    public partial class ConnectionMultiplexer
    {

        /// <summary>
        /// Create a sentinel monitor.
        /// </summary>
        public async Task<ISentinelSubscriber> CreateSentinelSubscriberAsync(ConfigurationOptions configurationOptions)
        {
            if (configurationOptions == null) throw new ArgumentNullException(nameof(configurationOptions));
            if (this.ServerSelectionStrategy.ServerType != ServerType.Sentinel)
            {
                throw new InvalidOperationException($@"The expected server type is {ServerType.Sentinel} but was {this.ServerSelectionStrategy.ServerType}.");
            }
            return await SentinelSubscriber.CreateAsync(this, configurationOptions).ForAwait();
        }

        internal class SentinelSubscriber : ISentinelSubscriber
        {

            public ConnectionMultiplexer Muxer { get; }
            public IReadOnlyDictionary<string, (ConnectionMultiplexer masterMuxer, ConnectionMultiplexer slavesMuxer)> Muxers { get; }

            public SentinelSubscriber(
                ConnectionMultiplexer muxer,
                IReadOnlyDictionary<string, (ConnectionMultiplexer masterMuxer, ConnectionMultiplexer slavesMuxer)> muxers)
            {
                this.Muxer = muxer ?? throw new ArgumentNullException(nameof(muxer));
                this.Muxers = muxers ?? throw new ArgumentNullException(nameof(muxers));
            }

            public IDatabase GetDatabase(string serviceName, SentinelDatabaseType databaseType, int db = -1, object asyncState = null)
            {
                if (string.IsNullOrWhiteSpace(serviceName)) throw new ArgumentNullException(nameof(serviceName));
                if (this.Muxers.TryGetValue(serviceName, out var service))
                {
                    return
                        databaseType == SentinelDatabaseType.Master ?
                        service.masterMuxer.GetDatabase(db, asyncState) :
                        service.slavesMuxer.GetDatabase(db, asyncState);
                }
                throw new KeyNotFoundException($@"The service ""{serviceName}"" was not found.");
            }

            public static async Task<SentinelSubscriber> CreateAsync(ConnectionMultiplexer muxer, ConfigurationOptions configurationOptions)
            {
                var tasks =
                    muxer._serverSnapshot.Span.ToArray()
                        .Where(serverEndPoint => serverEndPoint.ServerType == ServerType.Sentinel)
                        .Select(serverEndPoint => muxer.GetServer(serverEndPoint.EndPoint))
                        .Select(async server =>
                        {
                            var services = new List<(SentinelInstanceDetails master, IEnumerable<SentinelInstanceDetails> slaves)>();
                            var masters = await server.SentinelMastersAsync().ForAwait();
                            foreach (var master in masters.Select(kvps => new SentinelInstanceDetails(kvps)))
                            {
                                var slaves = await server.SentinelSlavesAsync(master.Name).ForAwait();
                                var service = (master, slaves: slaves.Select(kvps => new SentinelInstanceDetails(kvps)));
                                services.Add(service);
                            }
                            return services;
                        })
                        .ToList();
                for (var task = await Task.WhenAny(tasks).ForAwait(); tasks.Count > 0; await Task.WhenAny(tasks).ForAwait())
                {
                    tasks.Remove(task);
                    if (task.Status == TaskStatus.RanToCompletion)
                    {
                        var services = task.Result;
                        var muxers = new Dictionary<string, (ConnectionMultiplexer masterMuxer, ConnectionMultiplexer slavesMuxer)>(services.Count, StringComparer.InvariantCultureIgnoreCase);
                        foreach (var service in services)
                        {
                            var serviceName = service.master.Name;

                            var masterConfiguration = configurationOptions.Clone();
                            masterConfiguration.EndPoints.Clear();
                            masterConfiguration.EndPoints.Add(new IPEndPoint(service.master.Ip, service.master.Port));
                            var masterMuxer = await ConnectionMultiplexer.ConnectAsync(masterConfiguration).ForAwait();

                            var slavesConfiguration = configurationOptions.Clone();
                            slavesConfiguration.EndPoints.Clear();
                            foreach (var slave in service.slaves)
                            {
                                slavesConfiguration.EndPoints.Add(new IPEndPoint(slave.Ip, slave.Port));
                            }
                            var slavesMuxer = await ConnectionMultiplexer.ConnectAsync(slavesConfiguration).ForAwait();

                            muxers.Add(serviceName, (masterMuxer, slavesMuxer));
                        }
                        return new SentinelSubscriber(muxer, muxers);
                    }
                }
                throw new InvalidOperationException(@"Cannot obtain information from Sentinel servers.");
            }

        }

    }
}
