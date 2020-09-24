package work;

//classe q tem o objeto para aceder a base de dados

public class Item  {


    private String name;
    private double price;
    private int quantidade;

    public Item() {
        super();
    }

    public Item(String name, double price) {
        this.name = name;
        this.price = price;
        this.quantidade = quantidade;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(int quantidade) {
        this.quantidade = quantidade;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", price=" + price +
                ", quantidade=" + quantidade +
                '}';
    }
}
