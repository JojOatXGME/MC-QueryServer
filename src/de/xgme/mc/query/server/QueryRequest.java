package de.xgme.mc.query.server;

public class QueryRequest {
	private final String queryName;
	private final String[] args;

	QueryRequest(String commandLine) {
		String[] split = commandLine.trim().split("\\s+");
		this.queryName = split[0].toLowerCase();
		this.args = new String[split.length - 1];
		for (int i = 1; i < split.length; ++i) {
			args[i-1] = split[i];
		}
	}

	// ### public API ###

	public String getQueryName() {
		return queryName;
	}

	public String[] getArgs() {
		return args;
	}

	public String toString() {
		return getUniqueCommandLine();
	}

	// ### private methods ###

	private String getUniqueCommandLine() {
		String uniqueCommandLine = queryName;
		for (String arg : args) {
			uniqueCommandLine += " "+arg;
		}
		return uniqueCommandLine;
	}

}
