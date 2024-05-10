package org.wikidata.query.rdf.tool;

import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static org.wikidata.query.rdf.tool.StreamUtils.utf8;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import de.thetaphi.forbiddenapis.SuppressForbidden;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Utilities for command line scripts.
 */
public final class CliUtils {
    /**
     * Build a reader for the uri.
     *
     * @throws IOException if it is thrown opening the files and such
     */
    public static Reader reader(String uri) throws IOException {
        return utf8(inputStream(uri));
    }

    /**
     * Get an input stream for a uri. If the uri looks like a gzip file then
     * unzips it on the fly.
     *
     * @throws IOException if it is thrown opening the files and such
     */
    @SuppressFBWarnings(
            value = {"PATH_TRAVERSAL_IN", "URLCONNECTION_SSRF_FD"},
            justification = "coming from program arguments")
    public static InputStream inputStream(String uri) throws IOException {
        if (uri.equals("-")) {
            return ForbiddenOk.systemDotIn();
        }
        InputStream stream;
        if (!uri.contains(":/")) {
            stream = new BufferedInputStream(newInputStream(Paths.get(uri)));
        } else {
            stream = URI.create(uri).toURL().openStream();
        }
        if (uri.endsWith(".gz")) {
            stream = new GZIPInputStream(stream);
        } else if (uri.endsWith(".bz2")) {
            stream = new BZip2CompressorInputStream(stream);
        }

        return stream;
    }

    /**
     * Build a writer for the uri.
     *
     * @throws IOException if it is thrown opening the files and such
     */
    public static Writer writer(String uri) throws IOException {
        return utf8(outputStream(uri));
    }

    /**
     * Get an output stream for a file. If the file is - then returns stdin
     * instead. If the file looks like a gzip file then zips it on the fly. Also
     * creates the file file's parent directories if they don't already exist.
     *
     * @throws IOException if it is thrown opening the files and such
     */
    public static OutputStream outputStream(String out) throws IOException {
        if (out.equals("-")) {
            return ForbiddenOk.systemDotOut();
        }
        Path path = Paths.get(out);
        Path parent = path.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("Invalid path: " + out);
        }
        createDirectories(parent);
        OutputStream stream = new BufferedOutputStream(newOutputStream(path));
        if (out.endsWith(".gz")) {
            stream = new GZIPOutputStream(stream);
        }
        return stream;
    }

    /**
     * Methods in this class are ignored by the forbiddenapis checks. Thus you
     * need to really really really be sure what you are putting in here is
     * right.
     *
     * Methods on this class are public but a fairy dies every time you abuse
     * them.
     */
    @SuppressForbidden
    public static final class ForbiddenOk {
        private ForbiddenOk() {
            // Utility class should never be instantiated
        }
        /**
         * Get System.in. CliTools should be allowed to use System.in/out/err.
         */
        public static InputStream systemDotIn() {
            return System.in;
        }

        /**
         * Get System.out. CliTools should be allowed to use System.in/out/err.
         */
        public static PrintStream systemDotOut() {
            return System.out;
        }

        /**
         * Get System.err. CliTools should be allowed to use System.in/out/err.
         */
        public static PrintStream systemDotErr() {
            return System.err;
        }
    }

    private CliUtils() {
        // Uncallable utility constructor
    }
}
