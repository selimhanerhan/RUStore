package com.RUStore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import static com.RUStore.constants.*;

/* any necessary Java packages here */

public class RUStoreClient
{
    private Socket socket;
    private final String host;
    private final int port;

    private OutputStream out;
    private InputStream in;

    /**
     RUStoreClient Constructor, initializes default values
     for class members

     @param host host url
     @param port port number
     */
    public RUStoreClient ( final String host,
                           final int port )
    {
        this.host = host;
        this.port = port;
    }

    /**
     Opens a socket and establish a connection to the object store server
     running on a given host and port.

     @return n/a, however throw an exception if any issues occur
     */
    public void connect ()
            throws IOException
    {
        this.socket = new Socket();
        this.socket.connect( new InetSocketAddress( host, port ) );
        this.out = new BufferedOutputStream( socket.getOutputStream() );
        this.in = new BufferedInputStream( socket.getInputStream() );
    }

    /**
     Sends an arbitrary data object to the object store server. If an
     object with the same key already exists, the object should NOT be
     overwritten

     @param key key to be used as the unique identifier for the object
     @param data byte array representing arbitrary data object
     @return 0 upon success
     1 if key already exists
     Throw an exception otherwise
     */
    public int put ( final String key,
                     final byte[] data )
            throws IOException, InterruptedException
    {
        ByteBuffer buffer = ByteBuffer.allocate( oPut.getByteLength() + byteSize + key.getBytes().length + byteSize + data.length );
        buffer.putInt( oPut.getCode() );  // Operation code
        buffer.putInt( key.getBytes().length );    // Size of the key
        buffer.put( key.getBytes() );              // The key
        buffer.putInt( data.length );              // Size of the object
        buffer.put( data );                        // The object

        // Send the data object to the object store server.
        out.write( buffer.array() );
        out.flush();

        // Wait for the server to process the data before reading the response.
        Thread.sleep( 2000 );

        // Get the result code from the object store server.
        byte[] response = new byte[byteSize];
        in.read( response, 0, byteSize );

        return ByteBuffer.wrap( response ).getInt(); // either SUCCESS or KEY_ALREADY_EXIST
    }

    /**
     Sends an arbitrary data object to the object store server. If an
     object with the same key already exists, the object should NOT
     be overwritten.

     @param key key to be used as the unique identifier for the object
     @param file_path path of file data to transfer
     @return 0 upon success
     1 if key already exists
     Throw an exception otherwise
     */
    public int put ( final String key,
                     final String file_path )
            throws IOException, InterruptedException
    {
        File file = new File( file_path );
        byte[] data = new byte[( int ) file.length()];
        try ( FileInputStream in = new FileInputStream( file ) )
        {
            in.read( data );
        }

        return put( key, data );
    }

    /**
     Downloads arbitrary data object associated with a given key
     from the object store server.

     @param key key associated with the object
     @return object data as a byte array, null if key doesn't exist.
     Throw an exception if any other issues occur.
     */
    public byte[] get ( final String key )
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( oGet.getByteLength() + byteSize + key.getBytes().length );
        buffer.putInt( oGet.getCode() );  // Operation code
        buffer.putInt( key.getBytes().length );    // Size of the key
        buffer.put( key.getBytes() );              // The key

        out.write( buffer.array() );
        out.flush();

        byte[] response = new byte[byteSize];
        in.read( response );
        int resultCode = ByteBuffer.wrap( response ).getInt();

        if ( resultCode == existedKey )
        {
            response = new byte[byteSize];
            in.read( response );
            int objectLength = ByteBuffer.wrap( response ).getInt();

            response = new byte[objectLength];
            in.read( response );

            return ByteBuffer.wrap( response ).array();
        }
        else
        {
            return null;
        }
    }

    /**
     Downloads arbitrary data object associated with a given key
     from the object store server and places it in a file.

     @param key key associated with the object
     @param file_path output file path
     @return 0 upon success
     1 if key doesn't exist
     Throw an exception otherwise
     */
    public int get ( final String key,
                     final String file_path )
            throws IOException
    {
        byte[] data = get( key );

        if ( data == null )
        {
            return notExistedKey;
        }
        else
        {
            File file = new File( file_path );
            try ( FileOutputStream out = new FileOutputStream( file ) )
            {
                out.write( data );
                return success;
            }
        }
    }

    /**
     Removes data object associated with a given key
     from the object store server. Note: No need to download the data object,
     simply invoke the object store server to remove object on server side

     @param key key associated with the object
     @return 0 upon success
     1 if key doesn't exist
     Throw an exception otherwise
     */
    public int remove ( final String key )
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( oRemove.getByteLength() + byteSize + key.getBytes().length );
        buffer.putInt( oRemove.getCode() ); // Operation code
        buffer.putInt( key.getBytes().length );      // Size of the key
        buffer.put( key.getBytes() );                // The key

        out.write( buffer.array() );
        out.flush();

        byte[] response = new byte[byteSize];
        in.read( response );

        return ByteBuffer.wrap( response ).getInt();  
    }

    /**
     Retrieves of list of object keys from the object store server

     @return Array of keys as string array, null if there are no keys.
     Throw an exception if any other issues occur.
     */
    public String[] list ()
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( oList.getByteLength() );
        buffer.putInt( oList.getCode() ); // Operation code

        // Send the data object to the object store server.
        out.write( buffer.array() );
        out.flush();

        // Get the response
        byte[] response = new byte[byteSize];
        in.read( response );
        int listSize = ByteBuffer.wrap( response ).getInt();

        if ( listSize == 0 )
        {
            return null;
        }

        response = new byte[listSize];
        in.read( response );

        ByteArrayInputStream inputStream = new ByteArrayInputStream( response );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        int len;
        byte[] bytes = new byte[1024];
        while ( (len = inputStream.read( bytes, 0, bytes.length )) != -1 )
        {
            outputStream.write( bytes, 0, len );
        }

        return outputStream.toString().split( "\\|" );
    }

    /**
     Signals to server to close connection before closes
     the client socket.

     @return n/a, however throw an exception if any issues occur
     */
    public void disconnect ()
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( oDisconnect.getByteLength() );
        buffer.putInt( oDisconnect.getCode() ); // Operation code

        out.write( buffer.array() );
        out.flush();

        in.close();
        out.close();
        socket.close();
    }
}
