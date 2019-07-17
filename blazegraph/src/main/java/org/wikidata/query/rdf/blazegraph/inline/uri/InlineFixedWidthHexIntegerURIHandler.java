/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.wikidata.query.rdf.blazegraph.inline.uri;

import java.math.BigInteger;

import com.bigdata.rdf.internal.InlineSignedIntegerURIHandler;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 * Inline URI Handler to handle URI's in the form of a Hex fixed width integer.
 * Such as:
 *  <pre>
 *   http://test.com/element/d95dde070543a0e0115c8d5061fce6754bb82280
 *  </pre>
 *
 * Ref: https://phabricator.wikimedia.org/T213375
 *
 * @author <a href="mailto:igorkim78@gmail.com">Igor Kim</a>
 *
 */


public class InlineFixedWidthHexIntegerURIHandler extends
		InlineSignedIntegerURIHandler {

	private final int fixedWidth;
	private final String zeroPadding;

	public InlineFixedWidthHexIntegerURIHandler(final String namespace, final int fixedWidth) {
		super(namespace);
		this.fixedWidth = fixedWidth;
		this.zeroPadding = Strings.repeat("0", fixedWidth);
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected AbstractLiteralIV createInlineIV(String localName) {
		return super.createInlineIV(new BigInteger(localName, 16));
	}

	@Override
	public String getLocalNameFromDelegate(
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {

		final BigInteger bigintVal;
		if (delegate instanceof XSDIntegerIV) {
			// Getting BigInteger directly from IV
			bigintVal = ((XSDIntegerIV<?>)delegate).getInlineValue();
		} else {
			// Need to recode localName from IV
			final String intStr = super.getLocalNameFromDelegate(delegate);

			// radix 10 here because localName from delegate is encoded to decimal number by XSDIntegerIV
			bigintVal = new BigInteger(intStr, 10);

		}

		return toFixedWidthHexString(bigintVal);
	}

	@SuppressFBWarnings(value = {"STT_STRING_PARSING_A_FIELD", "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"},
			justification = "Using String in this manner is exactly what we want")
	private String toFixedWidthHexString(final BigInteger bigintVal) {
		String localName = bigintVal.toString(16);
		if (localName.length() < fixedWidth) {
			localName = zeroPadding.substring(localName.length()) + localName;
		}
		return localName;
	}

}
