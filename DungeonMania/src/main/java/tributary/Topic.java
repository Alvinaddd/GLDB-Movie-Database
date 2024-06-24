package tributary;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    private String topicId;
    private String topicType;
    private Map<String, Partition> partitions;
    
    public Topic(String id, String type) {
        this.topicId = id;
        this.topicType = type;
        this.partitions = new HashMap<>();
    }
  
    public Partition createPartition(String topicId, String partitionId) {
        //check if partition already exist
        if (hasPartition(partitionId)) {
            System.out.println("partition with id: " + partitionId + " already exist in topic: " + topicId);
            return null;
        }
        Partition partition = new Partition(topicId, partitionId);
        partitions.put(partitionId, partition);
        System.out.println("Created partition with id: " + partitionId + " in topic: " + topicId);
        return partition;
    }
    public Partition getPartitionById(String id) {
        return partitions.get(id);
    }

    public String getId() {
        return topicId;
    }

    public String getType() {
        return topicType;
    }

    // check if that partititon contains in that Topics
    public boolean hasPartition(String partitionId) {
        return partitions.containsKey(partitionId);
    }
}