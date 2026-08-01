import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PaletteExport } from './palette-export';

describe('PaletteExport', () => {
  let component: PaletteExport;
  let fixture: ComponentFixture<PaletteExport>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PaletteExport],
    }).compileComponents();

    fixture = TestBed.createComponent(PaletteExport);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
