import { Component, inject, model } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MAT_DIALOG_DATA, MatDialogActions, MatDialogClose, MatDialogContent, MatDialogTitle } from '@angular/material/dialog';
import { PaletteModel } from '../../services/color.state';

@Component({
  selector: 'pbu-palette-save-dialog',
  imports: [
    MatButtonModule,
    MatDialogTitle,
    MatDialogContent,
    MatDialogActions,
    MatDialogClose
  ],
  templateUrl: './palette-save-dialog.html',
  styleUrl: './palette-save-dialog.css',
})
export class PaletteSaveDialog {
  data = inject<{palette: PaletteModel}>(MAT_DIALOG_DATA);
  palette = model(this.data.palette);
}
