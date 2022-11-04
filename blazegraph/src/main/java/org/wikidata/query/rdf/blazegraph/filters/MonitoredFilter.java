package org.wikidata.query.rdf.blazegraph.filters;

import java.lang.management.ManagementFactory;

import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MonitoredFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(MonitoredFilter.class);

    /** The object name under which the stats for this filter are exposed through JMX. */
    @Nullable
    protected ObjectName objectName;

    /**
     * Register a Filter as an MBean.
     *
     * On successful registration, the {@link ObjectName} used for registration
     * is stored into an instance field so that the MBean can be released on
     * {@link Filter#destroy()}.
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String filterName = filterConfig.getFilterName();
        ObjectName name = null;
        try {
            name = new ObjectName(this.getClass().getName(), "filterName", filterName);
            MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
            platformMBeanServer.registerMBean(this, name);
            LOG.info("ThrottlingFilter MBean registered as {}.", name);
        } catch (MalformedObjectNameException e) {
            LOG.error("filter name {} is invalid as an MBean property.", filterName, e);
        } catch (InstanceAlreadyExistsException e) {
            LOG.error("MBean for {}} has already been registered.", filterName, e);
        } catch (NotCompliantMBeanException | MBeanRegistrationException e) {
            LOG.error("Could not register MBean for Filter {}.", filterName, e);
        }
        objectName = name;
    }

    /** Unregister MBean. */
    @Override
    public void destroy() {
        // Don't do anything if the MBean isn't registered.
        if (objectName != null) {
            MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
            try {
                platformMBeanServer.unregisterMBean(objectName);
                LOG.info("ThrottlingFilter MBean {} unregistered.", objectName);
            } catch (InstanceNotFoundException e) {
                LOG.warn("MBean already unregistered.", e);
            } catch (MBeanRegistrationException e) {
                LOG.error("Could not unregister MBean.", e);
            }
        }
        objectName = null;
    }
}
