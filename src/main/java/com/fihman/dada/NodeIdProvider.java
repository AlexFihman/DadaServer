package com.fihman.dada;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.UUID;

public class NodeIdProvider {
    private static final Logger logger = LoggerFactory.getLogger(NodeIdProvider.class);

    //todo: parameter
    private static final String NODE_ID_FILE = "node-id.txt";  // Path to the file that will store the UUID
    private static UUID nodeId;

    // Method to get or generate the NodeId
    public static UUID getNodeId() {
        if (nodeId == null) {
            // Load the UUID from the file if it exists
            nodeId = loadNodeId();
            if (nodeId == null) {
                // If not found, generate a new random UUID and save it
                nodeId = UUID.randomUUID();
                saveNodeId(nodeId);
            }
        }
        return nodeId;
    }

    // Load the NodeId from a file
    private static UUID loadNodeId() {
        File file = new File(NODE_ID_FILE);
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String uuidString = reader.readLine();
                return UUID.fromString(uuidString);
            } catch (IOException e) {
                logger.error("loadNodeId",e);
            }
        }
        return null;
    }

    // Save the NodeId to a file
    private static void saveNodeId(UUID nodeId) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODE_ID_FILE))) {
            writer.write(nodeId.toString());
        } catch (IOException e) {
            logger.error("saveNodeId",e);
        }
    }
}
