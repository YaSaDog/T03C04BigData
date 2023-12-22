namespace T03C04BigData
{
    public class Director
    {
        public int Id { get; set; }
        public string? Name { get; set; }

        public HashSet<Movie> Movies { get; set; } = new();

        public override string ToString()
        {
            return Name != null ? Name : "null";
        }

        public Director() { }
        public Director(string name)
        {
            Name = name;
        }
    }
}
