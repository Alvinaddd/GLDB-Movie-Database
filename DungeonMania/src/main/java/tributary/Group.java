package tributary;

import java.util.HashMap;
import java.util.Map;

public class Group {
    private String groupId;
    private String rebalancing;
    private String topicId;
    private Map<String, Consumer> consumers;

    public Group (String groupId, String topicId, String rebalancing) {
        this.groupId = groupId; 
        this.topicId = topicId; 
        this.rebalancing = rebalancing; 
        this.consumers = new HashMap<>();
    }

    public Consumer createConsumer( String consumerId) {
        //check if consumer already exist
        if (hasConsumer(consumerId)) {
            System.out.println("consumer with id: " + consumerId + " already exist in topic: " + getGroupId());
            return null;
        }
        Consumer consumer = new Consumer(getGroupId(), consumerId);
        consumers.put(consumerId, consumer);
        System.out.println("Created consumer with id: " + consumerId + " in group: " + groupId);
        return consumer;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getRebalancing() {
        return rebalancing;
    }

    public void setRebalancing(String rebalancing) {
        this.rebalancing = rebalancing;
    }

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }
  
    // check if consumer contain in that group
    public boolean hasConsumer(String consumerId) {
        return consumers.containsKey(consumerId);
    }

    // delete consumer
    public void deleteConsumer(String consumerId) {
        consumers.remove(consumerId);
        System.out.println("deleted cosumer id: " + consumerId + " in group: " + getGroupId());
    }
}
