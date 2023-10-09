package com.RUStore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static com.RUStore.constants.*;

/* any necessary Java packages here */

public class RUStoreServer {

	/* any necessary class members here */
	private static final HashMap<String, byte[]> objects = new HashMap<>();
	private static boolean running = true;
	/* any necessary helper methods here */



	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 * @throws InterruptedException
	 */
	public static void main(String args[]) throws InterruptedException, IOException{
		if ( args.length != 1 )
        {
            System.out.println( "Invalid number of arguments. You must provide a port number." );
            return;
        }

        int port = Integer.parseInt( args[0] );
        	
    	ServerSocket serverSocket = new ServerSocket( port );
        System.out.println( "RUStore server started on port " + port );

        while ( running )
        {
            System.out.println( "Waiting for client connection..." );
            
        	Socket socket = serverSocket.accept();
            OutputStream out = new BufferedOutputStream( socket.getOutputStream() );
            InputStream in = new BufferedInputStream( socket.getInputStream());
            System.out.println( "Client connected from " + socket.getInetAddress().getHostAddress() );

            boolean clientConnected = true;
            while ( clientConnected )
            {
                byte[] request = new byte[byteSize];
                in.read( request );
                int operationCode = ByteBuffer.wrap( request ).getInt();

                if ( operationCode == oPut.getCode() )
                {
                    put(in, out);
                }
                else if ( operationCode == oGet.getCode() )
                {
                    get( in, out );
                }
                else if ( operationCode == oRemove.getCode() )
                {
                    remove( in, out );
                }
                else if ( operationCode == oList.getCode() )
                {
                    list( out );
                }
                else if ( operationCode == oDisconnect.getCode() )
                {
                    System.out.println( "connection closed." );
                    clientConnected = false;
                }

                // Short delay to avoid busy-waiting while wait for the next incoming request
                Thread.sleep( 500 );
            }
            
        }
        
        
	

	}

	private static void list(OutputStream out2) throws IOException {
		ByteBuffer buffer;

        if ( objects.isEmpty() )
        {
            System.out.println( "no key exists" );
            buffer = ByteBuffer.allocate( byteSize );
            buffer.putInt( 0 );      
        }
        else
        {
            String[] keys = objects.keySet().toArray( new String[0] );

            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            for ( String key : keys )
            {
                byte[] bytes = (key + keySeparator).getBytes();
                arrayOutputStream.write( bytes, 0, bytes.length );
            }
            byte[] data = arrayOutputStream.toByteArray();

            System.out.println( "sending list of keys" );

            buffer = ByteBuffer.allocate( byteSize + data.length );
            buffer.putInt( data.length );        
            buffer.put( data );                  
        }

        out2.write( buffer.array() );
        out2.flush();
	}

	private static void remove(InputStream in2, OutputStream out2) throws IOException {
		byte[] request;

        request = new byte[byteSize];
        in2.read( request );
        int keySize = ByteBuffer.wrap( request ).getInt();
        request = new byte[keySize];
        in2.read( request );
        String key = new String( request, StandardCharsets.UTF_8 );

        ByteBuffer buffer = ByteBuffer.allocate( byteSize );

        if ( objects.containsKey( key ) )
        {
            System.out.println( "removing\"" + key + "\"" );

            objects.remove( key );

            buffer.putInt( success );  
		}      
        else
        {
            System.out.println( "key does not exist" );
            buffer.putInt( notExistedKey );  
        }

        out2.write( buffer.array() );
        out2.flush();
	}

	private static void get(InputStream in2, OutputStream out2) throws IOException {
		byte[] request;

        request = new byte[byteSize];
        in2.read( request );
        int sizeOfKey = ByteBuffer.wrap( request ).getInt();

        request = new byte[sizeOfKey];
        in2.read( request );
        String key = new String( request, StandardCharsets.UTF_8 );

        ByteBuffer buffer;

        if ( objects.containsKey( key ) )
        {
            System.out.println( "getting\"" + key + "\"" );

            final byte[] data = objects.get( key );

            buffer = ByteBuffer.allocate( byteSize + byteSize + data.length );
            buffer.putInt( existedKey );  
            buffer.putInt( data.length );        
            buffer.put( data );                  
        }
        else
        {
            System.out.println( "key does not exist" );
            buffer = ByteBuffer.allocate( byteSize );
            buffer.putInt( notExistedKey );      
        }

        out2.write( buffer.array() );
        out2.flush();
	}

	private static void put(InputStream in2, OutputStream out2) throws IOException {
		byte[] request;

		request = new byte[byteSize];
		in2.read(request);
		int keySize = ByteBuffer.wrap(request).getInt();

		request = new byte[keySize];
		in2.read(request);
		
		String key = new String(request, StandardCharsets.UTF_8);
		ByteBuffer buffer = ByteBuffer.allocate(byteSize);
		if(objects.containsKey(key)){
			System.out.println("key already exists.");
			buffer.putInt(existedKey);
		}
		else{
			request = new byte[byteSize];
			in2.read(request);
			int objectSize = ByteBuffer.wrap(request).getInt();

			request = new byte[objectSize];
			in2.read(request);
			byte[] data = ByteBuffer.wrap(request).array();

			System.out.println("put\""+key+"\"");
			objects.put(key,data);
			buffer.putInt(success);
		}
		out2.write(buffer.array());
		out2.flush();
	}
}
