package com.iamninad.geode.model;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

import java.io.Serializable;

public class ActorModel implements Serializable, PdxSerializable {
    private String id;
    private String name;
    private int birthYear;
    private int deathYear;
    private String primaryProfession;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getBirthYear() {
        return birthYear;
    }

    public void setBirthYear(int birthYear) {
        this.birthYear = birthYear;
    }

    public int getDeathYear() {
        return deathYear;
    }

    public void setDeathYear(int deathYear) {
        this.deathYear = deathYear;
    }

    public String getPrimaryProfession() {
        return primaryProfession;
    }

    public void setPrimaryProfession(String primaryProfession) {
        this.primaryProfession = primaryProfession;
    }


    @Override
    public void toData(PdxWriter writer) {
        writer.writeString("id", this.id);
        writer.writeString("name", this.name);
        writer.writeInt("birthYear", this.birthYear);
        writer.writeInt("deathYear", this.deathYear);
        writer.writeString("primaryProfession", this.primaryProfession);
    }

    @Override
    public void fromData(PdxReader reader) {
        ActorModel model = new ActorModel();
        model.setId(reader.readString("id"));
        model.setName(reader.readString("name"));
        model.setBirthYear(reader.readInt("birthYear"));
        model.setDeathYear(reader.readInt("deathYear"));
        model.setPrimaryProfession(reader.readString("primaryProfession"));

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActorModel that = (ActorModel) o;

        if (birthYear != that.birthYear) return false;
        if (deathYear != that.deathYear) return false;
        if (!id.equals(that.id)) return false;
        if (!name.equals(that.name)) return false;
        return primaryProfession != null ? primaryProfession.equals(that.primaryProfession) : that.primaryProfession == null;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
