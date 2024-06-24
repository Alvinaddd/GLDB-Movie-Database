package tributary;

public class Producer {
    private String producerId; 
    private String type; 
    private String allocation; 

    public Producer( String producerId, String type, String allocation) {
        this.producerId = producerId; 
        this.type = type; 
        this.allocation = allocation; 
    }

    public String getProducerId() {
        return producerId;
    }

    public String getType() {
        return type;
    }

    public String getAllocation() {
        return allocation;
    }
  
}
