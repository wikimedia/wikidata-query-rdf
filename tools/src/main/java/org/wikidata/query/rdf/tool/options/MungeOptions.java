package org.wikidata.query.rdf.tool.options;

import com.lexicalscope.jewel.cli.Option;

/**
 * CLI options for use with JewelCli.
 */
@SuppressWarnings("checkstyle:javadocmethod")
public interface MungeOptions extends OptionsUtils.BasicOptions, OptionsUtils.MungerOptions, OptionsUtils.WikibaseOptions {
    @Option(shortName = "f", defaultValue = "-", description = "Source file (or uri) to munge. Default is - aka stdin.")
    String from();

    @Option(shortName = "t", defaultValue = "-", description = "Destination of munge. Use port:<port_number> to start an "
            + "http server on that port. Default is - aka stdout. If the file's parent directories don't exist then they "
            + "will be created ala mkdir -p.")
    String to();

    @Option(defaultValue = "0", description = "Chunk size in entities. If specified then the \"to\" option must be a java "
            + "format string containing a single format identifier which is replaced with the chunk number port:<port_numer>. "
            + "%08d.ttl is a pretty good choice for format string. If \"to\" is in port form then every http request will "
            + "get the next chunk. Must be greater than 0 and less than " + Integer.MAX_VALUE + ".")
    int chunkSize();
}
