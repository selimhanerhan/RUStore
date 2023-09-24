package com.RUStore;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

/* any necessary Java packages here */

public class RUStoreServer {

	/* any necessary class members here */
	private static ServerSocket svc;
	private static Socket req;
	private static BufferedReader in;
	private static DataOutputStream out;
	/* any necessary helper methods here */

	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 */
	public static void main(String args[]){

		// Check if at least one argument that is potentially a port number
		if(args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}


		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);


		// Implement here //
		try {
			svc = new ServerSocket(port);
			System.out.println("Connecting to server at localhost:" + port);
			System.out.println("Connection established.");
			
			req = svc.accept();
			in = new BufferedReader(new InputStreamReader(req.getInputStream()));
			out = new DataOutputStream(req.getOutputStream());
			
			String line = in.readLine();
			String result = line.length() + ": " + line.toUpperCase() + '\n';
			out.writeBytes(result);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}
