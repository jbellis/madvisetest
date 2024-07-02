package org.example;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class Main {

    private static final String FILENAME = "/home/ubuntu/cassandra/data/data/coherebench/embeddings_table-a5b9e560347b11efaf001bb676958899/cc-3ghc_1q72_2xo0g2nt06q1h93ee1-bti-SAI+ca+embeddings_table_embedding_idx+TermsData.db";
    private static final int READ_SIZE = 10;

    private interface CLibrary extends com.sun.jna.Library {
        int madvise(Pointer addr, long length, int advice);

        CLibrary INSTANCE = Native.load("c", CLibrary.class);
    }

    public static void main(String[] args) {
        try {
            File file = new File(FILENAME);
            long fileSize = file.length();
            System.out.println("File Size: " + fileSize); // Debug print

            try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
                MappedByteBuffer fileMap = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

                // Apply MADV_RANDOM to the memory-mapped region
                int result = CLibrary.INSTANCE.madvise(Native.getDirectBufferPointer(fileMap), fileSize, 1 /* MADV_RANDOM */);
                if (result != 0) {
                    throw new IOException("Error applying MADV_RANDOM: " + result);
                }

                readRandomBytes(fileMap, fileSize);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void readRandomBytes(MappedByteBuffer fileMap, long fileSize) {
        Random random = new Random();
        int checksum = 0;

        byte[] buffer = new byte[READ_SIZE];
        var forever = true;
        while (forever) {
            long randomPosition = generateRandomPosition(fileSize - READ_SIZE + 1, random);
            fileMap.position((int) randomPosition);
            fileMap.get(buffer, 0, READ_SIZE);

            for (int i = 0; i < READ_SIZE; i++) {
                checksum += buffer[i];
            }
        }
        System.out.println("Checksum: " + checksum);
    }

    private static long generateRandomPosition(long maxValue, Random random) {
        return Math.abs(random.nextLong()) % maxValue;
    }
}
