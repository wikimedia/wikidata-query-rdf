/**
 * Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.
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
/*
 * Created on Apr 22, 2019
 */

package org.wikidata.query.rdf.blazegraph.ast.eval;

import org.wikidata.query.rdf.blazegraph.inline.uri.InlineFixedWidthHexIntegerURIHandler;

import com.bigdata.rdf.internal.InlineURIFactory;

@SuppressWarnings({"checkstyle:typename"})
public class T213375_UriInlineFactory extends InlineURIFactory {

    public T213375_UriInlineFactory() {
        addHandler(new InlineFixedWidthHexIntegerURIHandler("http://www.wikidata.org/value/", 40));
        addHandler(new InlineFixedWidthHexIntegerURIHandler("http://www.wikidata.org/reference/", 40));
    }

}
