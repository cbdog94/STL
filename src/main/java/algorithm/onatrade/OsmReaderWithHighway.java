package algorithm.onatrade;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link OsmReaderWithHighway} is used to save the map from wayID to its name.
 *
 * @author Bin Cheng
 */
public class OsmReaderWithHighway implements Sink {

    private Map<Long, String> highway = new HashMap<>();

    @Override
    public void process(EntityContainer entityContainer) {
        if (entityContainer instanceof WayContainer) {
            Way way = ((WayContainer) entityContainer).getEntity();
            for (Tag tag : way.getTags()) {
                if ("highway".equalsIgnoreCase(tag.getKey())) {
                    highway.put(way.getId(), tag.getValue());
                    break;
                }
            }
        }
    }

    @Override
    public void initialize(Map<String, Object> map) {

    }

    @Override
    public void complete() {

    }

    @Override
    public void release() {

    }

    public Map<Long, String> getHighway() {
        return highway;
    }

}
