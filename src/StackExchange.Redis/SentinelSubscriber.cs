using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
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
    /// The event arguments when the SentinelSubscriber receive a message "switch-master". 
    /// </summary>
    public sealed class SentinelSwitchMasterEventArgs : EventArgs
    {
        /// <summary>
        /// The name of service.
        /// </summary>
        public string MasterName { get; }
        /// <summary>
        /// The IP endpoint of the old master.
        /// </summary>
        public IPEndPoint OldEndPoint { get; }
        /// <summary>
        /// The IP endpoint of the new master.
        /// </summary>
        public IPEndPoint NewEndPoint { get; }

        internal SentinelSwitchMasterEventArgs(string masterName, IPEndPoint oldEndPoint, IPEndPoint newEndPoint)
        {
            this.MasterName = masterName;
            this.OldEndPoint = oldEndPoint;
            this.NewEndPoint = newEndPoint;
        }
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
        /// <summary>
        /// Occurs when a "switch-master" message is received.
        /// </summary>
        event EventHandler<SentinelSwitchMasterEventArgs> SwitchMaster;
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

        internal sealed class SentinelSubscriber : ISentinelSubscriber
        {

            private class Service
            {
                public string Name { get; }
                public ConnectionMultiplexer MasterMuxer { get; private set; }
                public ConnectionMultiplexer SlavesMuxer { get; private set; }

                public Service(string name, ConnectionMultiplexer masterMuxer, ConnectionMultiplexer slavesMuxer)
                {
                    this.Name = !string.IsNullOrEmpty(name) ? name : throw new ArgumentNullException(nameof(name));
                    if (masterMuxer == null) throw new ArgumentNullException(nameof(masterMuxer));
                    if (slavesMuxer == null) throw new ArgumentNullException(nameof(slavesMuxer));
                    this.Update(masterMuxer, slavesMuxer);
                }

                public void Update(ConnectionMultiplexer masterMuxer, ConnectionMultiplexer slavesMuxer)
                {
                    if (masterMuxer != null) this.MasterMuxer = masterMuxer;
                    if (slavesMuxer != null) this.SlavesMuxer = slavesMuxer;
                }
            }

            private ConnectionMultiplexer Muxer { get; }
            private IReadOnlyDictionary<string, Service> Services { get; }

            private ISubscriber Subscriber { get; }

            private SentinelSubscriber(
                ConnectionMultiplexer muxer,
                IReadOnlyDictionary<string, Service> services)
            {
                this.Muxer = muxer ?? throw new ArgumentNullException(nameof(muxer));
                this.Services = services ?? throw new ArgumentNullException(nameof(services));

                this.Subscriber = this.Muxer.GetSubscriber();

                // switch-master <master name> <oldip> <oldport> <newip> <newport>
                // The master new IP and address is the specified one after a configuration change.
                this.Subscriber.Subscribe(@"+switch-master", (channel, message) =>
                {
                    var arguments = ((string)message).Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (arguments.Length == 5 &&
                        IPAddress.TryParse(arguments[1], out var oldIp) && ushort.TryParse(arguments[2], out var oldPort) &&
                        IPAddress.TryParse(arguments[3], out var newIp) && ushort.TryParse(arguments[4], out var newPort))
                    {
                        Task.Run(() =>
                            this.OnSwitchMasterAsync(
                                new SentinelSwitchMasterEventArgs(
                                    masterName: arguments[0],
                                    oldEndPoint: new IPEndPoint(oldIp, oldPort),
                                    newEndPoint: new IPEndPoint(newIp, newPort))));
                    }
                });
            }

            public IDatabase GetDatabase(string serviceName, SentinelDatabaseType databaseType, int db = -1, object asyncState = null)
            {
                if (string.IsNullOrWhiteSpace(serviceName)) throw new ArgumentNullException(nameof(serviceName));
                if (this.Services.TryGetValue(serviceName, out var service))
                {
                    return
                        databaseType == SentinelDatabaseType.Master ?
                        service.MasterMuxer.GetDatabase(db, asyncState) :
                        service.SlavesMuxer.GetDatabase(db, asyncState);
                }
                throw new KeyNotFoundException($@"The service ""{serviceName}"" was not found.");
            }

            public event EventHandler<SentinelSwitchMasterEventArgs> SwitchMaster;

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
                for (var task = await Task.WhenAny(tasks).ForAwait(); tasks.Count > 0; task = await Task.WhenAny(tasks).ForAwait())
                {
                    tasks.Remove(task);
                    if (task.Status == TaskStatus.RanToCompletion)
                    {
                        var result = task.Result;
                        var services = new Dictionary<string, Service>(result.Count, StringComparer.InvariantCultureIgnoreCase);
                        foreach (var (master, slaves) in result)
                        {
                            var service = await CreateServiceAsync(configurationOptions, master, slaves).ForAwait();
                            services.Add(service.Name, service);
                        }
                        return new SentinelSubscriber(muxer, services);
                    }
                }
                throw new InvalidOperationException(@"Cannot obtain information from Sentinel servers.");
            }

            private async Task OnSwitchMasterAsync(SentinelSwitchMasterEventArgs e)
            {
                if (!string.IsNullOrWhiteSpace(e?.MasterName) && this.Services.TryGetValue(e.MasterName, out var outdatedService))
                    try
                    {
                        var tasks =
                            this.Muxer._serverSnapshot.Span.ToArray()
                                .Where(serverEndPoint => serverEndPoint.ServerType == ServerType.Sentinel)
                                .Select(serverEndPoint => this.Muxer.GetServer(serverEndPoint.EndPoint))
                                .Select(async server =>
                                {
                                    var master = new SentinelInstanceDetails(await server.SentinelMasterAsync(outdatedService.Name).ForAwait());
                                    var slaves = (await server.SentinelSlavesAsync(outdatedService.Name).ForAwait()).Select(kvps => new SentinelInstanceDetails(kvps));
                                    return (master, slaves);
                                })
                                .ToList();
                        for (var task = await Task.WhenAny(tasks).ForAwait(); tasks.Count > 0; task = await Task.WhenAny(tasks).ForAwait())
                        {
                            tasks.Remove(task);
                            if (task.Status == TaskStatus.RanToCompletion)
                            {
                                var (master, slaves) = task.Result;
                                var service = await CreateServiceAsync(outdatedService.MasterMuxer.RawConfig, master, slaves).ForAwait();
                                outdatedService.Update(service.MasterMuxer, service.SlavesMuxer);
                                break;
                            }
                        }
                    }
                    catch (Exception exception)
                    {
                        this.Muxer.Trace(exception.ToString());
                    }
                if (this.SwitchMaster != null)
                    try
                    {
                        this.SwitchMaster?.Invoke(this, e);
                    }
                    catch { }  // None of my business
            }

            private static async Task<Service> CreateServiceAsync(
                ConfigurationOptions configurationOptions,
                SentinelInstanceDetails master,
                IEnumerable<SentinelInstanceDetails> slaves)
            {
                var serviceName = master.Name;

                var masterConfiguration = configurationOptions.Clone();
                masterConfiguration.EndPoints.Clear();
                masterConfiguration.EndPoints.Add(new IPEndPoint(master.Ip, master.Port));
                var masterMuxer = await ConnectionMultiplexer.ConnectAsync(masterConfiguration).ForAwait();

                var slavesConfiguration = configurationOptions.Clone();
                slavesConfiguration.EndPoints.Clear();
                foreach (var slave in slaves)
                {
                    slavesConfiguration.EndPoints.Add(new IPEndPoint(slave.Ip, slave.Port));
                }
                var slavesMuxer = await ConnectionMultiplexer.ConnectAsync(slavesConfiguration).ForAwait();

                return new Service(serviceName, masterMuxer, slavesMuxer);
            }

        }

    }
}
