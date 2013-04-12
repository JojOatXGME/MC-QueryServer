package de.xgme.mc.query.server;

import java.io.BufferedWriter;
import java.io.IOException;

public class QueryResponse {

	public enum Status {

		OK                    (200, "OK"),
		FORBIDDED             (403, "Forbidden"),
		NOT_FOUND             (404, "Not Found"),
		INTERNAL_SERVER_ERROR (500, "Internal Server Error");

		private final int sc;
		private final String message;
		Status(int sc, String message) {
			this.sc = sc;
			this.message = message;
		}
	}

	// ### object specific ###

	private final BufferedWriter output;
	private boolean finished = false;
	private Status status;
	private StringBuilder responseMessage;

	QueryResponse(BufferedWriter output) {
		this.output = output;
		reset();
	}

	void send() throws IOException {
		if (finished) throw new IllegalStateException("Response already finished");
		finished = true;
		
		final String message = responseMessage.toString();
		output.write(status.sc + " " + status.message + "\n\r");
		output.write("Length: "+message.length()+"\n\r");
		output.write("\n\r");
		output.write(message);
		output.write("\n\r");
		
		output.flush();
		
		this.status = null;
		this.responseMessage = null;
	}

	// ### public API ###

	public void print(String str) {
		responseMessage.append(str);
	}

	public void print(Object obj) {
		responseMessage.append(obj);
	}

	public void printLine(String str) {
		print(str);
		printLine();
	}

	public void printLine(Object obj) {
		print(obj);
		printLine();
	}

	public void printLine() {
		responseMessage.append("\n\r");
	}

	public String getContent() {
		return responseMessage.toString();
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status statur) {
		if (finished) throw new IllegalStateException("Response already finished");
		this.status = statur;
	}

	public void reset() {
		if (finished) throw new IllegalStateException("Response already finished");
		status = Status.OK;
		responseMessage = new StringBuilder();
	}

}
