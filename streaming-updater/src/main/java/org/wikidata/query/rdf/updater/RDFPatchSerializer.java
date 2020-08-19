package org.wikidata.query.rdf.updater;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.tool.rdf.Patch;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RDFPatchSerializer extends Serializer<Patch> {
    // use flink's java serializer (ref FLINK-6025)
    public static final JavaSerializer<Statement> JAVA_SERIALIZER = new JavaSerializer<>();
    public static final int VERSION = 1;

    @Override
    public void write(Kryo kryo, Output output, Patch o) {
        output.writeInt(VERSION);
        writeOptionalCollection(kryo, output, o.getAdded());
        writeOptionalCollection(kryo, output, o.getLinkedSharedElements());
        writeOptionalCollection(kryo, output, o.getRemoved());
        writeOptionalCollection(kryo, output, o.getUnlinkedSharedElements());
    }

    @Override
    public Patch read(Kryo kryo, Input input, Class<Patch> aClass) {
        int v = input.readInt();
        if (v != VERSION) {
            throw new KryoException("Unsupported version " + v + " for " + Patch.class);
        }

        return new Patch(readOptionalList(kryo, input), readOptionalList(kryo, input),
                readOptionalList(kryo, input), readOptionalList(kryo, input));
    }

    private List<Statement> readOptionalList(Kryo kryo, Input input) {
        int s = input.readInt();
        if (s < 0) return null;
        List<Statement> stmts = new ArrayList<>(s);
        for (int i = 0; i < s; i++) {
            stmts.add(kryo.readObjectOrNull(input, Statement.class, JAVA_SERIALIZER));
        }
        return Collections.unmodifiableList(stmts);
    }

    private void writeOptionalCollection(Kryo kryo, Output output, Collection<Statement> statementList) {
        if (statementList == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(statementList.size());
        for (Statement s: statementList) {
            kryo.writeObject(output, s, JAVA_SERIALIZER);
        }
    }
}
