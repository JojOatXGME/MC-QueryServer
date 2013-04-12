package de.xgme.mc.query.sample;

import org.bukkit.OfflinePlayer;
import org.bukkit.Server;

import de.xgme.mc.query.server.QueryHandler;
import de.xgme.mc.query.server.QueryRequest;
import de.xgme.mc.query.server.QueryResponse;

public class ListPlayers implements QueryHandler {
	private final Server server;

	public ListPlayers(Server server) {
		this.server = server;
	}

	@Override
	public void handleQuery(QueryRequest request, QueryResponse response) {
		final String[] args = request.getArgs();
		if (args.length == 0) {
			listOnlinePlayers(response);
		} else if (args.length == 1 && args[0].equalsIgnoreCase("all")) {
			listAllPlayers(response);
		} else {
			sendError(response, "Wrong Arguments");
		}
	}

	private void listOnlinePlayers(QueryResponse response) {
		listPlayers(response, server.getOnlinePlayers());
	}

	private void listAllPlayers(QueryResponse response) {
		listPlayers(response, server.getOfflinePlayers());
	}

	private void listPlayers(QueryResponse response, OfflinePlayer[] players) {
		for (OfflinePlayer player : players) {
			response.printLine(player.getName());
		}
	}

	private void sendError(QueryResponse response, String message) {
		response.setStatus(QueryResponse.Status.NOT_FOUND); // TODO use other status
		response.printLine(message);
	}

}
