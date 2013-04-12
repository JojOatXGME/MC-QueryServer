package de.xgme.mc.query.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitScheduler;

import de.xgme.mc.query.server.QueryResponse.Status;

public class QueryServer {

	private final InetSocketAddress bindAddr;
	private final int backlog;
	private final Plugin plugin;
	private final Logger logger;
	private final BukkitScheduler scheduler;
	private final Map<String,MyHandler> handlers;
	
	private Executor executor = null;

	private Listener listener = null;
	private ConnectionCleaner connCleaner = null;

	public QueryServer(final InetSocketAddress bindAddr, final int backlog, Plugin plugin) {
		this.bindAddr = bindAddr;
		this.backlog = backlog;
		this.plugin = plugin;
		this.logger = plugin.getLogger();
		this.scheduler = plugin.getServer().getScheduler();
		this.handlers = new ConcurrentHashMap<String,MyHandler>();
	}

	public void setExecutor(final Executor executor) {
		if (executor == null)
			throw new IllegalArgumentException("Executor cannot be null");
		
		this.executor = executor;
	}

	public void addQueryType(final String queryName, final QueryHandler handler, boolean sync) {
		if (sync) {
			handlers.put(queryName.toLowerCase(), new MyHandler() {
				@Override
				void handle(final QueryRequest request, final QueryResponse response) throws Throwable {
					Future<Void> future = scheduler.callSyncMethod(plugin, new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							handler.handleQuery(request, response);
							return null;
						}
					});
					while (true) {
						boolean interrupted = false;
						try {
							future.get();
						} catch (ExecutionException e) {
							throw e.getCause();
						} catch (InterruptedException e) {
							interrupted = true;
							continue;
						}
						if (interrupted) Thread.currentThread().interrupt();
						break;
					}
				}
			});
		} else {
			handlers.put(queryName.toLowerCase(), new MyHandler() {
				@Override
				void handle(QueryRequest request, QueryResponse response) {
					handler.handleQuery(request, response);
				}
			});
		}
	}

	public void removeQueryType(String queryName) {
		handlers.remove(queryName.toLowerCase());
	}

	public boolean queryTypeExist(String queryName) {
		return handlers.containsKey(queryName.toLowerCase());
	}

	public boolean isRunning() {
		return (listener != null);
	}

	public synchronized void start() {
		if (isRunning()) return;
		if (executor == null) throw new IllegalStateException("Executor must be set to start");
		
		connCleaner = new ConnectionCleaner();
		listener = new Listener();
		listener.start();
		connCleaner.start();
	}

	public synchronized void stop() {
		if (!isRunning())
			throw new IllegalStateException("QueryServer is not running");
		
		listener.interrupt();
		try {
			listener.join(20*1000); // TODO use a time from configuration
		} catch (InterruptedException e) {
			logger.warning("Thread was interrupted while waiting for stopping the query connection listener");
			Thread.currentThread().interrupt();
		}
		
		if (listener != null && listener.isAlive()) {
			logger.warning("The query connection listener is still running. The system will proceed anyway");
		}
		connCleaner.stopAll();
	}

	private class Listener extends Thread {
		@Override
		public void run() {
			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(bindAddr.getPort(), backlog, bindAddr.getAddress());
				serverSocket.setSoTimeout(4096);
				while (!isInterrupted()) {
					Socket clientSocket = null;
					try {
						logger.finer("Waiting for new connections");
						clientSocket = serverSocket.accept();
						logger.finer("Connection to "+clientSocket.getInetAddress()+" accepted");
						executor.execute(new ConnectionHandler(clientSocket));
					} catch (SocketTimeoutException e) {
						// do nothing
					} catch (InterruptedIOException e) {
						interrupt();
					} catch (Exception e) {
						logger.log(Level.WARNING,
								"Error occurred by setting up connection to client ("+clientSocket.getInetAddress()+")",
								e);
					}
				}
			} catch (IOException e) {
				logger.log(Level.SEVERE,
						"Error occurred by starting the query server",
						e);
			} finally {
				if (serverSocket != null) {
					try {
						serverSocket.close();
					} catch (IOException e) { }
				}
				listener = null;
			}
		}
	}

	private class ConnectionHandler implements Runnable {
		private final Socket clientSocket;
		private int queryCount = 0;

		private ConnectionHandler(final Socket socket) {
			this.clientSocket = socket;
		}

		@Override
		public void run() {
			BufferedReader input  = null;
			BufferedWriter output = null;
			try {
				input  = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));
				output = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
				
				while (!Thread.currentThread().isInterrupted()) {
					connCleaner.register(ConnectionHandler.this);
					final String commandLine = input.readLine();
					queryCount++;
					if (commandLine == null) break;

					synchronized (ConnectionHandler.this) {
						// create request and response object
						final QueryRequest request = new QueryRequest(commandLine);
						final QueryResponse response = new QueryResponse(output);
						
						// log (accepted)
						logger.finer("query accepted: "+request);
						
						// handle query
						try {
							if (request.getQueryName().equals("login")) {
								
							} else if (request.getQueryName().equals("exit")) {
								response.addToMessage("Close Connection");
								try {
									response.send();
								} catch (IOException e) {}
								break;
							} else {
								final MyHandler myHandler = handlers.get(request.getQueryName());
								if (myHandler != null) {
									myHandler.handle(request, response);
								} else {
									response.setStatus(Status.NOT_FOUND);
								}
							}
						} catch (Throwable t) {
							logger.log(Level.SEVERE,
									"Uncaught exception by handling query ("+request+")",
									t);
							response.reset();
							response.setStatus(Status.INTERNAL_SERVER_ERROR);
						}
						response.send();
					}
				}
			} catch (UnsupportedEncodingException e) {
				logger.log(Level.SEVERE,
						"Could not set up query connection. Your Java does not support UTF-8?",
						e);
			} catch (IOException e) {
				if (!e.getMessage().equals("socket closed")) {
					logger.log(Level.WARNING,
							"Error occurred in communication with client",
							e);
				}
			} finally {
				if (input != null) {
					try { input.close(); } catch (IOException e) { }
				}
				if (output != null) {
					try { output.close(); } catch (IOException e) { }
				}
				try { clientSocket.close(); } catch (IOException e) {}
			}
		}

		private synchronized void close() {
			logger.finest("Close connection to "+clientSocket.getInetAddress());
			try {
				clientSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private class ConnectionCleaner extends Thread {
		Queue<CleanRequest> cleanRequests = new ConcurrentLinkedQueue<CleanRequest>();
		private boolean stopAll = false;

		@Override
		public void run() {
			CleanRequest cr;
			while (!stopAll) {
				cr = cleanRequests.poll();
				if (cr == null) {
					try {
						sleep(4096);
					} catch (InterruptedException e) { stopAll(); }
					continue;
				}
				if (cr.handler.queryCount != cr.queryCount) continue;
				long sleepTime = cr.cleanTime-System.currentTimeMillis();
				if (sleepTime > 0) {
					try { sleep(sleepTime); }
					catch (InterruptedException e) { stopAll(); }
				}
				if (cr.handler.queryCount != cr.queryCount) continue;
				cr.handler.close();
			}
			while ((cr = cleanRequests.poll()) != null) {
				if (cr.handler.clientSocket.isClosed()) continue;
				cr.handler.close();
			}
		}
		private void register(ConnectionHandler handler) {
			cleanRequests.offer(new CleanRequest(handler));
		}
		private void stopAll() {
			stopAll = true;
			interrupt();
		}

		private class CleanRequest {
			private final ConnectionHandler handler;
			private final long cleanTime;
			private final int queryCount;
			private CleanRequest(ConnectionHandler handler) {
				this.handler = handler;
				this.queryCount = handler.queryCount;
				this.cleanTime = System.currentTimeMillis()+30*60*1000;
			}
		}
	}

	private static abstract class MyHandler {
		abstract void handle(QueryRequest request, QueryResponse response)
				throws Throwable;
	}

}
