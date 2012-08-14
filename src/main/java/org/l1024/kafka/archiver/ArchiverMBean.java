package org.l1024.kafka.archiver;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: lorenz
 * Date: 8/10/12
 * Time: 4:16 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ArchiverMBean {

    public Set<String> getTopics();

    public int getTotalPartitions();

    public long getThreadRestarts();
}
