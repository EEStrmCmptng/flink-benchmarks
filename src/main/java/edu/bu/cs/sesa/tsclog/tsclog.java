package edu.bu.cs.sesa.tsclog;


import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class tsclog
{
    public static native int availcpus();
    public static native int cpu();
    public static native int tid();
    public static native void pin(int cpu);
    public static native long now();
    public static native long stdout_now();
    public static native long stdout_label_now(String label);
    public static native long stderr_now();
    public static native long stderr_label_now(String label);
    public static native long mklog(String name, long n,
				    int valsperentry,
				    int logonexit, int binary,
				    String valhdrs);
    public static native void log(long lptr);
    public static native void log1(long lptr, long v1);
    
    private long logptr = 0;
    
    static {
        try {
            System.load(extractLibrary("tsclog"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String extractLibrary(String name) throws IOException {
        Path libPath = Paths.get("/" + System.mapLibraryName(name));

        // Create temp file.
        File temp = File.createTempFile(name, ".tmp");
        temp.deleteOnExit(); // The file is deleted when JVM exits.

        // Write the library to the temp file.
        try (InputStream in = tsclog.class.getResourceAsStream(libPath.toString());
             OutputStream out = new FileOutputStream(temp)) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = in.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
        }

        return temp.getAbsolutePath();
    }

    public tsclog(String name, long n, int valsperentry, String valhdrs) {
	logptr = mklog(name,n,valsperentry,
		       1,  // logonexit
		       0,  // binary
		       valhdrs
		       );
	System.out.println("logptr: 0x" + Long.toUnsignedString(logptr,16));
    }

    public void log() {
	tsclog.log(logptr);

    }

    public void log1(long v1) {
	tsclog.log1(logptr,v1);
    }
}    
