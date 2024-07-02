package org.example;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Platform;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {

    private static final String FILENAME = "/home/ubuntu/cassandra/data/data/coherebench/embeddings_table-a5b9e560347b11efaf001bb676958899/cc-3ghc_1q72_2xo0g2nt06q1h93ee1-bti-SAI+ca+embeddings_table_embedding_idx+TermsData.db";
    private static final int READ_SIZE = 10;
    private static final long MAX_REGION_SIZE = 2L * 1024 * 1024 * 1024 - 4096; // aligned

    private interface CLibrary extends com.sun.jna.Library {
        int posix_madvise(Pointer addr, long length, int advice);

        CLibrary INSTANCE = Native.load("c", CLibrary.class);
    }

    private interface Errno extends com.sun.jna.Library {
        String strerror(int errno);

        Errno INSTANCE = Native.load(Platform.C_LIBRARY_NAME, Errno.class);
    }

    public static void main(String[] args) {
        try {
            File file = new File(FILENAME);
            long fileSize = file.length();
            System.out.println("File Size: " + fileSize); // Debug print

            try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
                List<MappedByteBuffer> regions = mapFileRegions(fileChannel, fileSize);
                readRandomBytes(regions, fileSize);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<MappedByteBuffer> mapFileRegions(FileChannel fileChannel, long fileSize) throws IOException {
        List<MappedByteBuffer> regions = new ArrayList<>();
        long position = 0;

        while (position < fileSize) {
            long regionSize = Math.min(MAX_REGION_SIZE, fileSize - position);
            MappedByteBuffer fileMap = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, regionSize);

            // Debugging print statements to verify arguments
            Pointer addr = Native.getDirectBufferPointer(fileMap);
            System.out.println("Pointer address: " + addr);
            System.out.println("Region size: " + regionSize);

            // Apply MADV_RANDOM to the memory-mapped region
            int result = CLibrary.INSTANCE.posix_madvise(addr, regionSize, 1 /* MADV_RANDOM */);
            if (result != 0) {
                int errno = Native.getLastError();
                String errorMessage = Errno.INSTANCE.strerror(errno);
                throw new IOException("Error applying MADV_RANDOM: errno=" + errno + ", message=" + errorMessage);
            }

            regions.add(fileMap);
            position += regionSize;
        }

        return regions;
    }

    private static void readRandomBytes(List<MappedByteBuffer> regions, long fileSize) {
        Random random = new Random();
        int checksum = 0;

        byte[] buffer = new byte[READ_SIZE];
        boolean forever = true;

        while (forever) {
            long randomPosition = generateRandomPosition(fileSize - READ_SIZE + 1, random);
            int regionIndex = (int) (randomPosition / MAX_REGION_SIZE);
            long regionOffset = randomPosition % MAX_REGION_SIZE;

            MappedByteBuffer fileMap = regions.get(regionIndex);
            fileMap.position((int) regionOffset);
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
