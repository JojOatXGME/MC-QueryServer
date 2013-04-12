package de.xgme.mc.query;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.bukkit.configuration.Configuration;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.server.PluginDisableEvent;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

import de.xgme.mc.query.server.QueryHandler;
import de.xgme.mc.query.server.QueryServer;

public class QueryServerPlugin extends JavaPlugin implements Listener {

	private QueryServer server = null;
	private ExecutorService executor = null;
	private final Set<String> queryNames = new HashSet<String>();
	private final Map<Plugin,Set<RegisteredQueryType>> pluginQueryMap = new HashMap<Plugin,Set<RegisteredQueryType>>();

	@Override
	public void onLoad() {
		saveDefaultConfig();
		reloadConfig();
	}

	@Override
	public void onEnable() {
		getServer().getPluginManager().registerEvents(this, this);
		startServer();
	}

	@Override
	public void onDisable() {
		stopServer();
	}

	@EventHandler
	public void onPluginDisable(PluginDisableEvent event) {
		Set<RegisteredQueryType> pluginQueries = pluginQueryMap.remove(event.getPlugin());
		if (pluginQueries != null) {
			for (RegisteredQueryType queryType : pluginQueries) {
				queryNames.remove(queryType.queryName.toLowerCase());
				server.removeQueryType(queryType.queryName);
			}
		}
	}

	public void registerQueryType(String query, QueryHandler handler,
			boolean runInPrimaryThread, Plugin plugin) {
		
		final String queryToLowerCase = query.toLowerCase();
		
		getLogger().fine("register query type: "+query+(runInPrimaryThread?" (synchron)":" (asynchron)"));
		
		if (queryNames.contains(queryToLowerCase)) {
			getLogger().log(Level.WARNING,
					"Query "+query+" from "+plugin+" was not registred. " +
							"There is already a other query with this name.");
			return;
		}
		
		final RegisteredQueryType queryType = new RegisteredQueryType(query, handler, plugin, runInPrimaryThread);
		Set<RegisteredQueryType> pluginQueries = pluginQueryMap.get(plugin);
		if (pluginQueries == null) {
			pluginQueries = new HashSet<RegisteredQueryType>();
			pluginQueryMap.put(plugin, pluginQueries);
		}
		pluginQueries.add(queryType);
		queryNames.add(queryToLowerCase);
		
		if (isServerRunning()) {
				server.addQueryType(queryToLowerCase, handler, runInPrimaryThread);
		}
	}

	private boolean isServerRunning() {
		return (server != null);
	}

	private void startServer() {
		if (isServerRunning()) {
			getLogger().finest("startServer() was called but the query server is already running");
			return;
		}
		
		// get server configuration
		final Configuration config = getConfig();
		final String bind    = config.getString("socket.bind");
		final int    port    = config.getInt("socket.port");
		final int    backlog = config.getInt("socket.backlog");
		
		// create socket address from configuration
		final InetSocketAddress bindAddr;
		if (bind == null) bindAddr = new InetSocketAddress(port);
		else              bindAddr = new InetSocketAddress(bind, port);
		
		// log (set up server)
		getLogger().fine("Set up query server");
		
		// create server
		server = new QueryServer(bindAddr, backlog, this);
		
		// set up executor
		executor = Executors.newCachedThreadPool();
		server.setExecutor(executor);
		
		// add load registered query types to server
		for (Entry<Plugin,Set<RegisteredQueryType>> pluginQueries : pluginQueryMap.entrySet()) {
			if (pluginQueries.getKey().isEnabled()) {
				for (RegisteredQueryType queryType : pluginQueries.getValue()) {
					server.addQueryType(queryType.queryName, queryType.handler, queryType.sync);
				}
			} else {
				for (RegisteredQueryType queryType : pluginQueries.getValue()) {
					queryNames.remove(queryType.queryName.toLowerCase());
				}
				pluginQueries.getValue().clear();
			}
		}
		
		// log (start server)
		getLogger().fine("Start query server");
		
		// start server
		server.start();
	}

	private void stopServer() {
		if (!isServerRunning()) {
			getLogger().finest("stopServer() was called but no query server is running");
			return;
		}
		
		// get server configuration
		final Configuration config = getConfig();
		final int executorShutdownTimeout = config.getInt("executor.shutdown-timeout");
		
		// log (stop listener)
		getLogger().fine("Stop listener for new connections");
		
		// stop server
		server.stop();
		server = null;
		
		// log (close query connections)
		getLogger().fine("Close all query connections");
		
		// stop executor
		executor.shutdownNow();
		try {
			executor.awaitTermination(executorShutdownTimeout, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			getLogger().log(Level.WARNING,
							"The thread was interrupted while waiting to close query connections.",
							e);
		}
		if (!executor.isTerminated()) {
			getLogger().warning("Some connections are not closed yet");
		}
		executor = null;
	}

	private static class RegisteredQueryType {
		private final String queryName;
		private final QueryHandler handler;
		private final boolean sync;

		private RegisteredQueryType(String queryName, QueryHandler handler,
				Plugin plugin, boolean sync) {
			
			this.queryName = queryName;
			this.handler = handler;
			this.sync = sync;
		}
	}

}
