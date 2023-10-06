package com.RUStore;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static com.RUStore.constants.*;

/* any necessary Java packages here */

public class RUStoreServer {

	/* any necessary class members here */
	private static ServerSocket svc;
	private static Socket req;
	private static BufferedInputStream in;
	private static DataOutputStream out;
	private static final HashMap<String, byte[]> objects = new HashMap<>();
	/* any necessary helper methods here */

	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 * @throws InterruptedException
	 */
	public static void main(String args[]) throws InterruptedException{
		if(args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}
		int port = Integer.parseInt(args[0]);

		try {
			svc = new ServerSocket(port);
			System.out.println("Connecting to server at localhost:" + port);
			System.out.println("Connection established.");
			
			req = svc.accept();
			in = new BufferedInputStream(req.getInputStream());
			out = new DataOutputStream(req.getOutputStream());
			
			
			boolean connection = true;
			while(connection){
				byte[] request = new byte[BYTES_PER_INTEGER];
				in.read(request);
				int code = ByteBuffer.wrap(request).getInt();
				if(code == oPut.getCode()){
					put(in, out);
				}
				else if(code == oGet.getCode()){
					get(in,out);
				}
				else if (code == oRemove.getCode()){
					remove(in,out);
				}
				else if(code == oList.getCode()){
					list(out);
				}
				else if(code == oDisconnect.getCode()){
					System.out.println("failed");
					connection = false;
				}
				Thread.sleep(500);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	private static void list(DataOutputStream out2) throws IOException {
		ByteBuffer buffer;
        if ( objects.isEmpty() ) {
            System.out.println( "key doesn't exists" );
            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER );
            buffer.putInt( 0 );     
        }
        else{
            String[] keys = objects.keySet().toArray( new String[0] );

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for ( String key : keys )
            {
                byte[] bytes = (key + KEY_SEPARATOR).getBytes();
                outputStream.write( bytes, 0, bytes.length );
            }
            byte[] data = outputStream.toByteArray();

            System.out.println( "list of keys is sent" );

            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER + data.length );
            buffer.putInt( data.length );        
            buffer.put( data );                  
        }
        out.write( buffer.array() );
        out.flush();
	}

	private static void remove(BufferedInputStream in2, DataOutputStream out2) throws IOException {
		byte[] request;

        request = new byte[BYTES_PER_INTEGER];
        in.read( request );
        int keySize = ByteBuffer.wrap( request ).getInt();
        request = new byte[keySize];
        in.read( request );
        String key = new String( request, StandardCharsets.UTF_8 );

        ByteBuffer buffer = ByteBuffer.allocate( BYTES_PER_INTEGER );

        if ( objects.containsKey( key ) )
        {
            System.out.println( "removing\"" + key + "\"" );

            objects.remove( key );

            buffer.putInt( SUCCESS );  
		}      
        else
        {
            System.out.println( "key does not exist" );
            buffer.putInt( KEY_NOT_EXIST );  
        }

        out.write( buffer.array() );
        out.flush();
	}

	private static void get(BufferedInputStream in2, DataOutputStream out2) throws IOException {
		byte[] request;

        request = new byte[BYTES_PER_INTEGER];
        in2.read( request );
        int keySize = ByteBuffer.wrap( request ).getInt();

        request = new byte[keySize];
        in2.read( request );
        String key = new String( request, StandardCharsets.UTF_8 );

        ByteBuffer buffer;

        if ( objects.containsKey( key ) )
        {
            System.out.println( "getting\"" + key + "\"" );

            final byte[] data = objects.get( key );
            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER + BYTES_PER_INTEGER + data.length );
            buffer.putInt( KEY_ALREADY_EXIST );  
            buffer.putInt( data.length );        
            buffer.put( data );                  
        }
        else
        {
            System.out.println( "key does not exist" );
            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER );
            buffer.putInt( KEY_NOT_EXIST );      

        out.write( buffer.array() );
        out.flush();
		}
	}

	private static void put(BufferedInputStream in2, DataOutputStream out2) throws IOException {
		byte[] request;

		request = new byte[BYTES_PER_INTEGER];
		in2.read(request);
		int keySize = ByteBuffer.wrap(request).getInt();

		request = new byte[keySize];
		in2.read(request);
		
		String key = new String(request, StandardCharsets.UTF_8);
		ByteBuffer buffer = ByteBuffer.allocate(BYTES_PER_INTEGER);
		if(objects.containsKey(key)){
			System.out.println("key already exists.");
			buffer.putInt(KEY_ALREADY_EXIST);
		}
		else{
			request = new byte[BYTES_PER_INTEGER];
			in2.read(request);
			int objectSize = ByteBuffer.wrap(request).getInt();

			request = new byte[objectSize];
			in2.read(request);
			byte[] data = ByteBuffer.wrap(request).array();

			System.out.println("put\""+key+"\"");
			objects.put(key,data);
			buffer.putInt(SUCCESS);
		}
		out.write(buffer.array());
		out.flush();
	}

}
