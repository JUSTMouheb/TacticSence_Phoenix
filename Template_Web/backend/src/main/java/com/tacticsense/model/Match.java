public class Match {
    private Long id;
    private String date;
    private String teams;
    private String score;

    public Match() {
    }

    public Match(Long id, String date, String teams, String score) {
        this.id = id;
        this.date = date;
        this.teams = teams;
        this.score = score;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTeams() {
        return teams;
    }

    public void setTeams(String teams) {
        this.teams = teams;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }
}