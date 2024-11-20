from django import forms
from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.models import Biome


class BiomeForm(forms.ModelForm):
    parent = forms.ModelChoiceField(queryset=Biome.objects.all(), required=True)

    class Meta:
        model = Biome
        fields = ["biome_name", "parent"]

    def save_m2m(self):
        return self._save_m2m()

    def save(self, commit=True):
        parent = self.cleaned_data.get("parent")
        if not parent:
            parent = Biome.objects.roots.first()
        return Biome.objects.create_child(
            biome_name=self.cleaned_data["biome_name"], parent=parent
        )


@admin.register(Biome)
class BiomeAdmin(ModelAdmin):
    form = BiomeForm
    readonly_fields = ["pretty_lineage", "descendants_count"]
    search_fields = ["path", "biome_name"]
