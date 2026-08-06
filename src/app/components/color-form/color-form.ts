import { Component, computed, inject } from '@angular/core';
import { applyEach, form, FormField, max, min } from '@angular/forms/signals';
import { ColorState } from '../../services/color.state';
import { MatSliderModule } from '@angular/material/slider';
import { MatInputModule } from '@angular/material/input';
import { MatIcon } from '@angular/material/icon';
import { MatButton, MatIconButton } from '@angular/material/button';
import { MatToolbar } from '@angular/material/toolbar';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { PresetState } from '../../services/preset.state';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatDialog } from '@angular/material/dialog';
import { PaletteSaveDialog } from '../palette-save-dialog/palette-save-dialog';

@Component({
  selector: 'pbu-color-form',
  imports: [
    MatSliderModule, MatInputModule, FormField, MatIcon, MatIconButton, MatButton, MatToolbar, MatCheckboxModule
  ],
  templateUrl: './color-form.html',
  styleUrl: './color-form.css',
})
export class ColorForm {
  colorState = inject(ColorState)
  presetState = inject(PresetState)
  snackBar = inject(MatSnackBar)
  dialog = inject(MatDialog)

  stateForm = this.colorState.stateForm

  addShade = this.colorState.addShade;
  addColor = this.colorState.addColor;
  removeShade = this.colorState.removeShade;
  removeColor = this.colorState.removeColor;

  savePreset() {
    const palette = this.colorState.state();
    const exists = this.presetState.checkIfExists(palette)

    if (exists) {
      const dialogRef = this.dialog.open(PaletteSaveDialog, {
        data: {palette},
      });
      dialogRef.afterClosed().subscribe(({success})=>{
        if (success) {
          this.presetState.updatePreset(palette);
          this.snackBar.open(`Preset "${palette.name}" updated`, 'Close', {
            duration: 5000
          });
        }
      })
    }
    else {
      this.presetState.savePreset(palette)
      this.snackBar.open(`Preset "${palette.name}" saved`, 'Close', {
        duration: 5000
      });
    }
  }

  resetForm() {
    this.colorState.reset()
  }
}
