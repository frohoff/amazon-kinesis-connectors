package com.amazonaws.services.kinesis.connectors.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

public class HttpEmitter implements IEmitter<String> {
    private static final Log LOG = LogFactory.getLog(HttpEmitter.class);

	private final URL url;
	private final HttpMethod method;
	private final Set<Integer> successStatusCodes;
	private final Map<String, String> headers;

	public HttpEmitter(URL url, HttpMethod method, Set<Integer> successStatusCodes, Map<String, String> headers) {
		this.url = url;
		this.method = method;
		this.successStatusCodes = successStatusCodes;
		this.headers = headers;

		if (this.successStatusCodes.isEmpty()) this.successStatusCodes.addAll(Arrays.asList(200, 201, 202, 204));
		if (method == null) method = HttpMethod.POST;
	}

	public HttpEmitter(KinesisConnectorConfiguration configuration) {
		try {
			this.url = new URL(configuration.HTTP_URL);
		} catch (MalformedURLException e) {
			// FIXME:
			throw new RuntimeException(e);
		}
		this.method = HttpMethod.valueOf(configuration.HTTP_METHOD);
		this.successStatusCodes = parseIntSet(configuration.HTTP_SUCCESSFUL_STATUS_CODES);
		this.headers = parseHeaders(configuration.HTTP_HEADERS);
	}

	private Map<String, String> parseHeaders(String headersString) {
		Map<String, String> headers = new HashMap<String, String>();
		if (headersString == null) return headers;
		String[] headerStrings = headersString.split(",");
		for (String headerString : headerStrings) {
			String[] parts = headerString.split("=", 2);
			if (parts.length == 2) {
				headers.put(parts[0].trim(), parts[1].trim());
			}
		}
		return headers;
	}

	private Set<Integer> parseIntSet(String str) {
		Set<Integer> set = new HashSet<Integer>();
		if (str == null) return set;
		String[] strings = str.split(",");
		for (String string : strings) {
			try {
				set.add(Integer.parseInt(string.trim()));
			} catch (NumberFormatException e) {
				// ignore
			}

		}
		return set;
	}

	protected void prepareConnection(HttpURLConnection con) throws IOException {
		con.setRequestMethod(method.toString());
		for (Map.Entry<String,String> header : headers.entrySet()) {
			con.setRequestProperty(header.getKey(), header.getValue());
		}
	}

	protected boolean isFailed(HttpURLConnection con) throws IOException {
		int code = con.getResponseCode();
		return ! successStatusCodes.contains(code);
	}

	@Override
	public List<String> emit(UnmodifiableBuffer<String> buffer)	throws IOException {
		List<String> failed = new LinkedList<String>();
		for (String item : buffer.getRecords()) {
			try {
				HttpURLConnection con = (HttpURLConnection) url.openConnection();

				prepareConnection(con);

				con.setDoOutput(true);

				con.connect();

				OutputStream out = con.getOutputStream();
				byte[] bytes = item.getBytes();
				out.write(bytes);
				out.flush();
				out.close();

				// TODO: handle exception case
				if (isFailed(con)) {
					failed.add(item);
				} else {
					LOG.info("HttpEmitter emitted " + bytes.length + " bytes downstream to " + url + " with status " + con.getResponseCode());
				}
			} catch (Exception e) {
				LOG.error("error emitting to HTTP", e);
				failed.add(item);
			}
		}
		return failed;
	}

	@Override
	public void fail(List<String> records) {

	}

	@Override
	public void shutdown() {

	}

}
