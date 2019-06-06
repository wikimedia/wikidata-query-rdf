package org.wikidata.query.rdf.blazegraph;

import org.openrdf.query.BindingSet;

import com.bigdata.rdf.sparql.ast.service.RemoteServiceCall;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceCallImpl;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceFactoryImpl;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.codahale.metrics.Timer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * RemoteServiceFactoryImpl wrapper that times requests.
 */
public class MeteringRemoteServiceFactory extends RemoteServiceFactoryImpl {

    private final Timer requestTimer;

    public MeteringRemoteServiceFactory(Timer requestTimer, RemoteServiceOptions serviceOptions) {
        super(serviceOptions);
        this.requestTimer = requestTimer;
    }

    @Override
    public RemoteServiceCall create(final ServiceCallCreateParams params) {
        return new RemoteServiceCallImpl(params) {
            @Override
            public ICloseableIterator<BindingSet> call(final BindingSet[] bindingSets)
                    throws Exception {
                try (Timer.Context context = requestTimer.time()) {
                    return super.call(bindingSets);
                }
            }
        };
    }

}
