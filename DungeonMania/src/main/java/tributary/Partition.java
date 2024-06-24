package tributary;

public class Partition {
    private String partitionId; 
    private String topicId; 

    public Partition(String topicId, String partitionId) {
        this.topicId = topicId; 
        this.partitionId = partitionId; 
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getTopicId() {
        return topicId;
    } 
    
}
