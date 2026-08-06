import { Component, inject, input } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { PresetModel, PresetState } from '../../services/preset.state';
import { ColorState } from '../../services/color.state';
import { MatSnackBar } from '@angular/material/snack-bar';

@Component({
  selector: 'pbu-preset-card',
  imports: [MatCardModule, MatButton],
  templateUrl: './preset-card.html',
  styleUrl: './preset-card.css',
})
export class PresetCard {
  colorState = inject(ColorState)
  presetState = inject(PresetState)
  snackBar = inject(MatSnackBar)

  preset = input.required<PresetModel>()

  loadPreset(preset: PresetModel) {
    const {name, colors} = preset.palette
    this.colorState.setColor(name, colors)
    this.snackBar.open(`Preset "${name}" loaded`, 'Close', {
      duration: 5000
    });
  }

  deletePreset(preset: PresetModel) {
    this.presetState.deletePreset(preset)
    this.snackBar.open(`Preset "${preset.palette.name}" deleted`, 'Close', {
      duration: 5000
    });
  }
}
