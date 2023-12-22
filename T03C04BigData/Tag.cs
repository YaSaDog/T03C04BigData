namespace T03C04BigData
{
    public class Tag
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public HashSet<Movie> Movies { get; set; } = new();

        public override string ToString()
        {
            return Name != null ? Name : "null";
        }

        public Tag() { }
        public Tag(string name) 
        { 
            Name = name;
        }
    }
}
