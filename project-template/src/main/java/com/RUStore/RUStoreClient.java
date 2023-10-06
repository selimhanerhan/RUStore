package com.RUStore;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import static com.RUStore.constants.*;

/* any necessary Java packages here */

public class RUStoreClient {

	/* any necessary class members here */
	private final String host;
	private final int port;
	private Socket socket;
	private DataOutputStream out;
	private InputStream in;
	
	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) {
		this.host = host;
		this.port = port;
	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void connect()  {
		try {
			socket = new Socket(host, port);
			socket.connect(new InetSocketAddress(host,port));
			out = new DataOutputStream(socket.getOutputStream());
			in = new BufferedInputStream(socket.getInputStream());
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public int put(String key, byte[] data) throws IOException, InterruptedException {
		ByteBuffer buffer = ByteBuffer.allocate(oPut.getByteLength() + BYTES_PER_INTEGER + key.getBytes().length + BYTES_PER_INTEGER + data.length);
		buffer.putInt( oPut.getCode());
		buffer.putInt(key.getBytes().length);
		buffer.putInt(data.length);
		buffer.put(data);

		
		out.write(buffer.array());
		out.flush();
		Thread.sleep(200);
		byte[] response = new byte[BYTES_PER_INTEGER];
		in.read(response, 0 , BYTES_PER_INTEGER);

		
		// Implement here
		return ByteBuffer.wrap(response).getInt();

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws InterruptedException
	 */
	public int put(String key, String file_path) throws FileNotFoundException, IOException, InterruptedException {
		File file = new File(file_path);
		byte[] data = new byte[(int) file.length()];
		try(FileInputStream in = new FileInputStream(file)){
			in.read(data);
		}
		// Implement here
		return put(key, data);

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		object data as a byte array, null if key doesn't exist.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException
	 */
	public byte[] get(String key) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate( oGet.getByteLength() + BYTES_PER_INTEGER + key.getBytes().length);
		buffer.putInt(oGet.getCode());
		buffer.putInt(key.getBytes().length);
		buffer.put(key.getBytes());

		out.write(buffer.array());
		out.flush();

		byte[] response = new byte[BYTES_PER_INTEGER];
		in.read(response);
		int code = ByteBuffer.wrap(response).getInt();

		if(code == KEY_ALREADY_EXIST){
			response = new byte[BYTES_PER_INTEGER];
			in.read(response);
			int objectLength = ByteBuffer.wrap(response).getInt();
			response = new byte[objectLength];
			in.read(response);
			return ByteBuffer.wrap(response).array();
		}
		else{
			return null;
		}
		
		// Implement here

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public int get(String key, String file_path) throws FileNotFoundException, IOException {
		byte[] data = get(key);
		if(data == null){
			return KEY_NOT_EXIST;
		}
		else{
			File file = new File(file_path);
			try(FileOutputStream out = new FileOutputStream((file))){
				out.write(data);
				return SUCCESS;
			}
		}
		// Implement here

	}

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException
	 */
	public int remove(String key) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(oRemove.getByteLength() + BYTES_PER_INTEGER + key.getBytes().length);
		buffer.putInt(oRemove.getCode());
		buffer.putInt(key.getBytes().length);
		buffer.put(key.getBytes());

		out.write(buffer.array());
		out.flush();

		byte[] response = new byte[BYTES_PER_INTEGER];
		in.read(response);

		// Implement here
		return ByteBuffer.wrap(response).getInt();

	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException
	 */
	public String[] list() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(oList.getByteLength());
		buffer.putInt(oList.getCode());

		out.write(buffer.array());
		out.flush();

		byte[] response = new byte[BYTES_PER_INTEGER];
		in.read(response);
		int size = ByteBuffer.wrap( response).getInt();

		if(size == 0){
			return null;
		}

		response = new byte[size];
		in.read(response);
		ByteArrayInputStream input = new ByteArrayInputStream(response);
		ByteArrayOutputStream output = new ByteArrayOutputStream();

		int len;
		byte[] bytes = new byte[1024];
		while(( len = input.read(bytes, 0, bytes.length)) != -1){
			output.write(bytes, 0, len);
		}
		// Implement here
		return output.toString().split("\\/");

	}

	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void disconnect() {

		// Implement here
		try {
			ByteBuffer buffer = ByteBuffer.allocate( oDisconnect.getByteLength());
			buffer.putInt( oDisconnect.getCode());
			out.write(buffer.array());

			out.close();
			in.close();
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
