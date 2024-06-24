package tributary;

public class Consumer {
    private String groupId; 
    private String consumerId; 

    public Consumer(String groupId, String consumerId) {
        this.groupId = groupId; 
        this.consumerId = consumerId; 
    }

    public String getGroupId() {
        return groupId;
    }

    public String getConsumerId() {
        return consumerId;
    }
}
