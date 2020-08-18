package org.wikidata.query.rdf.tool.eventsummmary;

import org.wikidata.query.rdf.tool.options.OptionsUtils;

import com.lexicalscope.jewel.cli.Option;

public interface SummarizerOptions  extends OptionsUtils.BasicOptions {
        @Option(shortName = "e", defaultToNull = true, description = "Path to a file containing query events")
        String eventFilePath();

        @Option(shortName = "s", defaultToNull = true, description = "Path to an output summary")
        String summaryFilePath();
}
