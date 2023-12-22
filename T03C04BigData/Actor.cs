namespace T03C04BigData
{
    public class Actor
    {
        public int Id { get; set; }
        public string? Name { get; set; }

        public HashSet<Movie> Movies { get; set; } = new();

        public override string ToString()
        {
            return Name != null ? Name : "null";
        }

        public Actor() { }
        public Actor(string name)
        {
            Name = name;
        }
    }
}
