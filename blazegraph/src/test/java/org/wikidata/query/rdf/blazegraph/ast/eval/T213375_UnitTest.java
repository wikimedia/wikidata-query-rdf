/**
 * Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.
 * <p>
 * Contact:
 * SYSTAP, LLC DBA Blazegraph
 * 2501 Calvert ST NW #106
 * Washington, DC 20008
 * licenses@blazegraph.com
 * <p>
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.wikidata.query.rdf.blazegraph.ast.eval;

import java.math.BigInteger;
import java.util.Properties;

import org.junit.Test;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test case for https://phabricator.wikimedia.org/T213375 Inline value and reference URIs.
 *
 * @author <a href="mailto:igorkim78@gmail.com">Igor Kim</a>
 */
@SuppressWarnings("checkstyle:typename")
public class T213375_UnitTest extends AbstractDataDrivenSPARQLTestCase {

    public T213375_UnitTest() {
    }

    public T213375_UnitTest(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                T213375_Vocabulary.class.getName());

        properties.setProperty(AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS,
                T213375_UriInlineFactory.class.getName());

        return properties;

    }

    @SuppressWarnings("rawtypes")
    @Test
    public void test_reference_inlining() {
        // Prepare reference URI
        String reference = "http://www.wikidata.org/reference/";
        String id = "0123456789abcdef0123456789abcdef01234567";
        String uri = reference + id;
        // Pass URI to the journal for inlining
        IV<?, ?> iv = this.store.addTerm(new URIImpl(uri));
        // Test IV type
        assertEquals(URIExtensionIV.class, iv.getClass());
        // Test prefix is inlined as vocabulary IV
        IV extensionIV = ((URIExtensionIV) iv).getExtensionIV();
        assertEquals(VocabURIByteIV.class, extensionIV.getClass());
        // Test prefix encoded correctly in lexicon
        assertEquals(reference, extensionIV.asValue(this.store.getLexiconRelation()).stringValue());
        // Test local name is xsd:integer
        assertEquals(XSDIntegerIV.class, ((URIExtensionIV) iv).getLocalNameIV().getClass());
        // Test local name is encoded as hex BigInteger
        assertEquals(new BigInteger(id, 16), ((URIExtensionIV) iv).getLocalNameIV().integerValue());
        // Test string representation of the IV matches to reference URI
        assertEquals(uri, iv.asValue(this.store.getLexiconRelation()).stringValue());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void test_value_inlining() {
        // Prepare value URI
        String value = "http://www.wikidata.org/value/";
        String id = "0123456789abcdef0123456789abcdef01234567";
        String uri = value + id;
        // Pass URI to the journal for inlining
        IV<?, ?> iv = this.store.addTerm(new URIImpl(uri));
        // Test IV type
        assertEquals(URIExtensionIV.class, iv.getClass());
        // Test prefix is inlined as vocabulary IV
        IV extensionIV = ((URIExtensionIV) iv).getExtensionIV();
        assertEquals(VocabURIByteIV.class, extensionIV.getClass());
        // Test prefix encoded correctly in lexicon
        assertEquals(value, extensionIV.asValue(this.store.getLexiconRelation()).stringValue());
        // Test local name is xsd:integer
        assertEquals(XSDIntegerIV.class, ((URIExtensionIV) iv).getLocalNameIV().getClass());
        // Test local name is encoded as hex BigInteger
        assertEquals(new BigInteger(id, 16), ((URIExtensionIV) iv).getLocalNameIV().integerValue());
        // Test string representation of the IV matches to reference URI
        assertEquals(uri, iv.asValue(this.store.getLexiconRelation()).stringValue());
    }

    @Test
    public void test_T213375Testa() throws Exception {
        // Run integration AST Eval Test:
        //  - load data from nt
        //  - run SPARQL query
        //  - test if results match to expected
        String path = "/" + getClass().getPackage().getName().replaceAll("\\.", "/") + "/";
        new TestHelper("T213375a", // testURI,
                path + "T213375a.rq", // queryFileURL
                path + "T213375.nt", // dataFileURL
                path + "T213375.srx", // resultFileURL
                false /* checkOrder */
        ).runTest();
    }

}
