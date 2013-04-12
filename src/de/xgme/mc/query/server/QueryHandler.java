package de.xgme.mc.query.server;

public interface QueryHandler {

	void handleQuery(QueryRequest request, QueryResponse response);

}
